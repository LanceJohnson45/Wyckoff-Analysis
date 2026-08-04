from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import os
from typing import Optional

import numpy as np
import pandas as pd
from postgrest.exceptions import APIError
from supabase import Client

from core.constants import TABLE_STOCK_HIST_CACHE
from core.kline_quality import repair_ohlc_relationship
from integrations.postgres_base import connect_postgres, postgres_enabled, upsert_rows
from integrations.supabase_base import create_admin_client as _create_admin_client

_ADMIN_CLIENT: Client | None = None
_CLI_CLIENT: Client | None = None
_CLI_TOKENS: tuple[str, str] = ("", "")
_FUNNEL_TRADING_DAYS = max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30)
_DEFAULT_RETENTION_DAYS = max(int(_FUNNEL_TRADING_DAYS * 1.7), 540)
_STOCK_HIST_RETENTION_DAYS = max(
    int(os.getenv("STOCK_HIST_RETENTION_DAYS", str(_DEFAULT_RETENTION_DAYS))),
    30,
)


def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime strings and accept trailing 'Z' timezone markers."""
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


@dataclass
class CacheMeta:
    symbol: str
    adjust: str
    source: str
    start_date: date
    end_date: date
    updated_at: datetime


_COL_MAP = {
    "日期": "date",
    "开盘": "open",
    "最高": "high",
    "最低": "low",
    "收盘": "close",
    "成交量": "volume",
    "成交额": "amount",
    "涨跌幅": "pct_chg",
    "换手率": "turnover_rate",
    "振幅": "amplitude",
}


def _infer_amount_multiplier(
    close: pd.Series,
    volume: pd.Series,
    amount: pd.Series,
) -> float:
    close = pd.to_numeric(close, errors="coerce")
    volume = pd.to_numeric(volume, errors="coerce")
    amount = pd.to_numeric(amount, errors="coerce")
    estimated_base = close * volume
    valid_base = estimated_base.where(estimated_base > 0)
    ratio = (amount.where(amount > 0) / valid_base).replace([np.inf, -np.inf], np.nan)
    ratio = pd.to_numeric(ratio, errors="coerce").dropna()
    if ratio.empty:
        return 1.0
    median_ratio = float(ratio.median())
    candidates = (1.0, 100.0)
    return min(candidates, key=lambda x: abs(median_ratio - x))


def fill_missing_volume(out: pd.DataFrame) -> pd.DataFrame:
    if not {"close", "amount"}.issubset(out.columns):
        return out

    close = pd.to_numeric(out["close"], errors="coerce")
    amount = pd.to_numeric(out["amount"], errors="coerce")
    if "volume" in out.columns:
        volume = pd.to_numeric(out["volume"], errors="coerce")
    else:
        volume = pd.Series(pd.NA, index=out.index, dtype="Float64")

    valid_close = close.where(close > 0)
    fill_mask = (volume.isna() | (volume < 0)) & valid_close.notna() & (amount > 0)
    if not bool(fill_mask.any()):
        out["volume"] = volume
        return out

    multiplier = _infer_amount_multiplier(close, volume, amount)
    volume.loc[fill_mask] = amount.loc[fill_mask] / valid_close.loc[fill_mask] / multiplier
    out["volume"] = volume
    return out


def fill_missing_amount(out: pd.DataFrame) -> pd.DataFrame:
    if not {"close", "volume"}.issubset(out.columns):
        return out

    close = pd.to_numeric(out["close"], errors="coerce")
    volume = pd.to_numeric(out["volume"], errors="coerce")
    estimated_base = close * volume
    valid_base = estimated_base.where(estimated_base > 0)

    if "amount" in out.columns:
        amount = pd.to_numeric(out["amount"], errors="coerce")
    else:
        amount = pd.Series(pd.NA, index=out.index, dtype="Float64")

    fill_mask = (amount.isna() | (amount <= 0)) & valid_base.notna()
    if not bool(fill_mask.any()):
        out["amount"] = amount
        return out

    multiplier = _infer_amount_multiplier(close, volume, amount)
    amount.loc[fill_mask] = valid_base.loc[fill_mask] * multiplier
    out["amount"] = amount
    return out


def normalize_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(columns=_COL_MAP).copy()
    keep = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "turnover_rate",
        "amplitude",
    ]
    out = out[[c for c in keep if c in out.columns]].copy()
    
    # 如果缺少 amplitude，尝试计算
    if "amplitude" not in out.columns and all(c in out.columns for c in ["high", "low", "close"]):
        prev_close = out["close"].shift(1)
        base = prev_close.where(prev_close > 0, out["close"].where(out["close"] > 0, pd.NA))
        out["amplitude"] = ((out["high"] - out["low"]) / base.replace(0, pd.NA) * 100).astype(float)
    
    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "turnover_rate",
        "amplitude",
    ]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "date" in out.columns:
        out["date"] = out["date"].astype(str)
    out = fill_missing_volume(out)
    out = fill_missing_amount(out)
    out = repair_ohlc_relationship(out)
    return out


def _sanitize_payload_dataframe(payload: pd.DataFrame) -> pd.DataFrame:
    cleaned = payload.copy()

    float_cols = cleaned.select_dtypes(include=["floating"]).columns
    if len(float_cols) > 0:
        cleaned.loc[:, float_cols] = cleaned.loc[:, float_cols].replace(
            [np.inf, -np.inf], np.nan
        )

    cleaned = cleaned.astype(object)
    cleaned = cleaned.where(pd.notna(cleaned), None)

    return cleaned


def _collect_invalid_numeric_samples(payload: pd.DataFrame) -> dict[str, list[int]]:
    issues: dict[str, list[int]] = {}
    numeric_cols = payload.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        series = pd.to_numeric(payload[col], errors="coerce")
        invalid_mask = series.isna() | np.isinf(series.to_numpy(dtype=float, na_value=np.nan))
        invalid_rows = payload.index[invalid_mask].tolist()
        if invalid_rows:
            issues[col] = invalid_rows[:10]
    return issues


def denormalize_hist_df(df: pd.DataFrame) -> pd.DataFrame:
    reverse = {v: k for k, v in _COL_MAP.items()}
    out = df.rename(columns=reverse).copy()
    return out


def _parse_iso_date(value: str) -> date:
    return pd.to_datetime(str(value)).date()


def _get_admin_supabase_client() -> Client | None:
    global _ADMIN_CLIENT
    if _ADMIN_CLIENT is not None:
        return _ADMIN_CLIENT
    try:
        _ADMIN_CLIENT = _create_admin_client()
        if _ADMIN_CLIENT is None:
            print("[stock_cache] Admin client creation returned None", flush=True)
    except Exception as e:
        print(f"[stock_cache] Admin client creation failed: {e}", flush=True)
        _ADMIN_CLIENT = None
    return _ADMIN_CLIENT


def set_cli_tokens(access_token: str, refresh_token: str = "") -> None:
    """CLI 登录后调用，注入 user JWT 供缓存层使用。"""
    global _CLI_CLIENT, _CLI_TOKENS
    _CLI_TOKENS = (access_token, refresh_token)
    _CLI_CLIENT = None  # 下次调用时重建


def _get_cli_user_client() -> Client | None:
    global _CLI_CLIENT
    if _CLI_CLIENT is not None:
        return _CLI_CLIENT
    at, rt = _CLI_TOKENS
    if not at:
        return None
    try:
        from integrations.supabase_base import create_user_client
        _CLI_CLIENT = create_user_client(at, rt)
        return _CLI_CLIENT
    except Exception:
        return None


def _get_stock_cache_client(context: str = "auto") -> Client | None:
    """
    context:
    - web/session: 优先使用登录态 session client（RLS）
    - background/admin: 使用 service-role/admin client
    - auto: web session → CLI user client → admin client
    """
    ctx = str(context or "auto").strip().lower()
    try_session = ctx in {"auto", "web", "session"}
    try_admin = ctx in {"auto", "background", "admin"}

    if try_session:
        # 1) Streamlit session client
        try:
            from integrations.supabase_client import get_supabase_client
            client = get_supabase_client()
            if client is not None:
                return client
        except Exception:
            pass
        # 2) CLI user client (access_token)
        cli = _get_cli_user_client()
        if cli is not None:
            return cli
        if ctx in {"web", "session"}:
            return None

    if try_admin:
        return _get_admin_supabase_client()
    return None


def get_cache_meta(
    symbol: str, adjust: str, *, context: str = "auto"
) -> Optional[CacheMeta]:
    if postgres_enabled():
        try:
            with connect_postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    select min(date) as start_date,
                           max(date) as end_date,
                           max(updated_at) as updated_at
                    from public.stock_hist_cache
                    where symbol = %s and adjust = %s
                    """,
                    (symbol, adjust),
                )
                row = cur.fetchone()
            if not row or not row.get("start_date") or not row.get("end_date"):
                return None
            return CacheMeta(
                symbol=symbol,
                adjust=adjust,
                source="cache",
                start_date=row["start_date"],
                end_date=row["end_date"],
                updated_at=row.get("updated_at") or datetime.now(timezone.utc),
            )
        except Exception:
            return None

    supabase = _get_stock_cache_client(context=context)
    if supabase is None:
        return None
    try:
        first_resp = (
            supabase.table(TABLE_STOCK_HIST_CACHE)
            .select("date")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("date", desc=False)
            .limit(1)
            .execute()
        )
        if not first_resp.data:
            return None

        last_resp = (
            supabase.table(TABLE_STOCK_HIST_CACHE)
            .select("date,updated_at")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .order("date", desc=True)
            .limit(1)
            .execute()
        )
        if not last_resp.data:
            return None
    except APIError:
        return None
    except Exception:
        return None

    first_row = first_resp.data[0]
    last_row = last_resp.data[0]
    updated_raw = last_row.get("updated_at")
    updated_at = (
        _parse_iso_datetime(updated_raw) if updated_raw else datetime.now(timezone.utc)
    )
    return CacheMeta(
        symbol=symbol,
        adjust=adjust,
        source="cache",
        start_date=_parse_iso_date(first_row["date"]),
        end_date=_parse_iso_date(last_row["date"]),
        updated_at=updated_at,
    )


def load_cached_history(
    symbol: str,
    adjust: str,
    source: str,
    start_date: date,
    end_date: date,
    *,
    context: str = "auto",
) -> Optional[pd.DataFrame]:
    if postgres_enabled():
        try:
            with connect_postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    select date, open, high, low, close, volume, amount, pct_chg, turnover_rate, amplitude
                    from public.stock_hist_cache
                    where symbol = %s
                      and adjust = %s
                      and date >= %s
                      and date <= %s
                    order by date
                    """,
                    (symbol, adjust, start_date, end_date),
                )
                rows = cur.fetchall()
            return repair_ohlc_relationship(pd.DataFrame(rows)) if rows else None
        except Exception:
            return None

    supabase = _get_stock_cache_client(context=context)
    if supabase is None:
        return None
    try:
        resp = (
            supabase.table(TABLE_STOCK_HIST_CACHE)
            .select(
                "date,open,high,low,close,volume,amount,pct_chg,turnover_rate,amplitude"
            )
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .gte("date", start_date.isoformat())
            .lte("date", end_date.isoformat())
            .order("date")
            .execute()
        )
        if resp.data:
            return repair_ohlc_relationship(pd.DataFrame(resp.data))
    except APIError:
        return None
    except Exception:
        return None
    return None


def load_cached_dates(
    symbol: str,
    adjust: str,
    start_date: date,
    end_date: date,
    *,
    context: str = "auto",
) -> list[date]:
    if postgres_enabled():
        try:
            with connect_postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    select date
                    from public.stock_hist_cache
                    where symbol = %s
                      and adjust = %s
                      and date >= %s
                      and date <= %s
                    order by date
                    """,
                    (symbol, adjust, start_date, end_date),
                )
                rows = cur.fetchall()
            return sorted({row["date"] for row in rows if row.get("date")})
        except Exception:
            return []

    supabase = _get_stock_cache_client(context=context)
    if supabase is None:
        return []
    try:
        resp = (
            supabase.table(TABLE_STOCK_HIST_CACHE)
            .select("date")
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .gte("date", start_date.isoformat())
            .lte("date", end_date.isoformat())
            .order("date")
            .execute()
        )
    except APIError:
        return []
    except Exception:
        return []
    out: list[date] = []
    for row in resp.data or []:
        raw = None
        if isinstance(row, dict):
            raw = row.get("date")
        if not raw:
            continue
        try:
            out.append(_parse_iso_date(str(raw)))
        except Exception:
            continue
    return sorted(set(out))


def upsert_cache_data(
    symbol: str,
    adjust: str,
    source: str,
    df: pd.DataFrame,
    *,
    context: str = "auto",
) -> bool:
    if df is None or df.empty:
        return False

    # Drop rows with NULL/NaN OHLC to prevent caching bad data.
    # yfinance occasionally returns partial rows (volume present, OHLC=NaN)
    # during rate-limiting or before the daily bar is finalized.
    # Such rows pollute the cache permanently because cache_only mode
    # never re-fetches. Better to skip them and let the next prewarm fill the gap.
    _ohlc_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    if _ohlc_cols:
        _bad_mask = df[_ohlc_cols].isna().any(axis=1)
        _bad_count = int(_bad_mask.sum())
        if _bad_count > 0:
            print(
                f"[upsert_cache_data] Dropping {_bad_count} rows with NULL OHLC "
                f"for symbol={symbol}, adjust={adjust}, source={source}",
                flush=True,
            )
            df = df.loc[~_bad_mask].copy()
            if df.empty:
                print(
                    f"[upsert_cache_data] All rows had NULL OHLC, skipping cache write "
                    f"for symbol={symbol}, adjust={adjust}",
                    flush=True,
                )
                return False

    if postgres_enabled():
        payload = df.copy()
        payload["date"] = payload["date"].astype(str)
        payload["symbol"] = symbol
        payload["adjust"] = adjust
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        invalid_before_clean = _collect_invalid_numeric_samples(payload)
        payload = _sanitize_payload_dataframe(payload)
        records = payload.to_dict(orient="records")
        try:
            json.dumps(records, allow_nan=False)
            upsert_rows(
                TABLE_STOCK_HIST_CACHE,
                records,
                conflict_columns=("symbol", "adjust", "date"),
            )
            with connect_postgres() as conn, conn.cursor() as cur:
                _trim_symbol_history_window_pg(
                    cur=cur,
                    symbol=symbol,
                    adjust=adjust,
                    retention_days=_STOCK_HIST_RETENTION_DAYS,
                )
            return True
        except Exception as e:
            print(
                f"[upsert_cache_data] PG Exception: symbol={symbol}, adjust={adjust}, "
                f"source={source}, rows={len(records)}, error={type(e).__name__}: {e}",
                flush=True,
            )
            if invalid_before_clean:
                print(
                    f"[upsert_cache_data] Invalid numeric values before clean: {invalid_before_clean}",
                    flush=True,
                )
            return False

    supabase = _get_stock_cache_client(context=context)
    if supabase is None:
        return False

    payload = df.copy()
    payload["date"] = payload["date"].astype(str)
    payload["symbol"] = symbol
    payload["adjust"] = adjust
    payload["updated_at"] = datetime.now(timezone.utc).isoformat()
    invalid_before_clean = _collect_invalid_numeric_samples(payload)
    payload = _sanitize_payload_dataframe(payload)
    records = payload.to_dict(orient="records")

    try:
        json.dumps(records, allow_nan=False)
        supabase.table(TABLE_STOCK_HIST_CACHE).upsert(records).execute()
        _trim_symbol_history_window(
            supabase=supabase,
            symbol=symbol,
            adjust=adjust,
            retention_days=_STOCK_HIST_RETENTION_DAYS,
        )
        return True
    except APIError as e:
        error_msg = str(e)
        # 如果是列缺失错误，尝试移除该列后重试
        if "Could not find the" in error_msg and "column" in error_msg:
            import re
            match = re.search(r"'(\w+)' column", error_msg)
            if match:
                missing_col = match.group(1)
                print(
                    f"[upsert_cache_data] Column '{missing_col}' not in schema, retrying without it",
                    flush=True,
                )
                # 从 payload 中移除该列
                if missing_col in payload.columns:
                    payload = payload.drop(columns=[missing_col])
                    records = payload.to_dict(orient="records")
                    try:
                        supabase.table(TABLE_STOCK_HIST_CACHE).upsert(records).execute()
                        _trim_symbol_history_window(
                            supabase=supabase,
                            symbol=symbol,
                            adjust=adjust,
                            retention_days=_STOCK_HIST_RETENTION_DAYS,
                        )
                        return True
                    except Exception as retry_error:
                        print(
                            f"[upsert_cache_data] Retry failed: {retry_error}",
                            flush=True,
                        )
                        return False
        
        print(
            f"[upsert_cache_data] APIError: symbol={symbol}, adjust={adjust}, "
            f"source={source}, rows={len(records)}, error={e}",
            flush=True,
        )
        if invalid_before_clean:
            print(
                f"[upsert_cache_data] Invalid numeric values before clean: {invalid_before_clean}",
                flush=True,
            )
        return False
    except Exception as e:
        print(
            f"[upsert_cache_data] Exception: symbol={symbol}, adjust={adjust}, "
            f"source={source}, rows={len(records)}, error={type(e).__name__}: {e}",
            flush=True,
        )
        if invalid_before_clean:
            print(
                f"[upsert_cache_data] Invalid numeric values before clean: {invalid_before_clean}",
                flush=True,
            )
        for idx, record in enumerate(records[:5]):
            try:
                json.dumps(record, allow_nan=False)
            except Exception as record_error:
                print(
                    f"[upsert_cache_data] Bad record[{idx}] JSON serialization failed: "
                    f"{record_error}; record={record}",
                    flush=True,
                )
        return False


def _trim_symbol_history_window(
    *,
    supabase: Client,
    symbol: str,
    adjust: str,
    retention_days: int,
) -> None:
    cutoff_date = (
        datetime.utcnow().date() - timedelta(days=max(retention_days, 1))
    ).isoformat()
    try:
        (
            supabase.table(TABLE_STOCK_HIST_CACHE)
            .delete()
            .eq("symbol", symbol)
            .eq("adjust", adjust)
            .lt("date", cutoff_date)
            .execute()
        )
    except Exception:
        pass


def _trim_symbol_history_window_pg(
    *,
    cur,
    symbol: str,
    adjust: str,
    retention_days: int,
) -> None:
    cutoff_date = datetime.utcnow().date() - timedelta(days=max(retention_days, 1))
    cur.execute(
        """
        delete from public.stock_hist_cache
        where symbol = %s
          and adjust = %s
          and date < %s
        """,
        (symbol, adjust, cutoff_date),
    )

def upsert_cache_meta(
    symbol: str,
    adjust: str,
    source: str,
    start_date: date,
    end_date: date,
    *,
    context: str = "auto",
) -> None:
    # breaking change: 单表架构不再维护独立 meta 表
    _ = (symbol, adjust, source, start_date, end_date, context)
    return


def cleanup_cache(
    ttl_days: int = _STOCK_HIST_RETENTION_DAYS, *, context: str = "auto"
) -> None:
    if postgres_enabled():
        cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).date()
        try:
            with connect_postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    "delete from public.stock_hist_cache where date < %s",
                    (cutoff,),
                )
        except Exception:
            pass
        return

    supabase = _get_stock_cache_client(context=context)
    if supabase is None:
        return
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).date().isoformat()
    try:
        supabase.table(TABLE_STOCK_HIST_CACHE).delete().lt("date", cutoff).execute()
    except Exception:
        pass
