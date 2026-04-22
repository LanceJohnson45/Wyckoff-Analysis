# -*- coding: utf-8 -*-
"""
Supabase 推荐跟踪数据存取模块
"""
from __future__ import annotations

import os
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from supabase import Client
from core.constants import TABLE_RECOMMENDATION_TRACKING
from integrations.supabase_base import create_admin_client as _get_supabase_admin_client
from integrations.supabase_base import is_admin_configured as is_supabase_configured


def _parse_recommend_date(raw_value: Any) -> date | None:
    if raw_value is None:
        return None
    s = str(raw_value).strip()
    if not s:
        return None
    try:
        if len(s) == 8 and s.isdigit():
            return datetime.strptime(s, "%Y%m%d").date()
        return datetime.fromisoformat(s).date()
    except Exception:
        return None


def _parse_write_date(record: dict[str, Any]) -> date | None:
    """优先用 recommend_date，没有则回退 created_at。"""
    rec_date = _parse_recommend_date(record.get("recommend_date"))
    if rec_date is not None:
        return rec_date

    created = record.get("created_at")
    if created is not None and str(created).strip():
        try:
            s = str(created).strip()
            if "T" in s or " " in s:
                return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
            if len(s) == 8 and s.isdigit():
                return datetime.strptime(s, "%Y%m%d").date()
            return datetime.fromisoformat(s).date()
        except Exception:
            pass
    return None


def _normalize_market(raw_value: Any, *, default: str = "cn") -> str:
    market = str(raw_value or default or "cn").strip().lower()
    return market if market in {"cn", "us"} else default


def _normalize_symbol(raw_value: Any, *, market: str = "cn") -> str:
    market_norm = _normalize_market(market)
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if market_norm == "cn":
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return ""
        return digits[-6:].zfill(6)
    symbol = text.upper()
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-")
    if not symbol or not symbol[0].isalpha():
        return ""
    if any(ch not in allowed for ch in symbol):
        return ""
    return symbol[:15]


def _legacy_code_from_symbol(symbol: str, market: str) -> int | None:
    if _normalize_market(market) != "cn":
        return None
    normalized = _normalize_symbol(symbol, market="cn")
    if not normalized:
        return None
    try:
        return int(normalized)
    except Exception:
        return None


def _market_symbol_from_record(
    record: dict[str, Any],
    *,
    default_market: str = "cn",
) -> tuple[str, str]:
    market = _normalize_market(record.get("market"), default=default_market)
    symbol = _normalize_symbol(record.get("symbol"), market=market)
    if symbol:
        return market, symbol
    code_value = record.get("code")
    if code_value is not None and str(code_value).strip():
        legacy_symbol = _normalize_symbol(code_value, market="cn")
        if legacy_symbol:
            return "cn", legacy_symbol
    return market, ""


def _supports_market_symbol_schema(client: Client) -> bool:
    try:
        client.table(TABLE_RECOMMENDATION_TRACKING).select("id,market,symbol").limit(1).execute()
        return True
    except Exception:
        return False


def _extract_close_map_from_hist(hist: pd.DataFrame) -> dict[str, float]:
    if hist is None or hist.empty:
        return {}
    work = hist.copy()
    if "日期" not in work.columns or "收盘" not in work.columns:
        return {}
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    work["收盘"] = pd.to_numeric(work["收盘"], errors="coerce")
    work = work.dropna(subset=["日期", "收盘"])
    work = work[work["收盘"] > 0]
    if work.empty:
        return {}
    return {
        row["日期"].strftime("%Y%m%d"): float(row["收盘"])
        for _, row in work.sort_values("日期").iterrows()
    }


def _resolve_initial_price_from_history(
    symbol: str,
    rec_date: date,
    *,
    market: str = "cn",
) -> float:
    """
    用推荐日附近历史日线回填加入价：
    1) 优先 rec_date 当天
    2) 若当天无数据，回看最近 7 天并取 <= rec_date 的最近交易日
    """
    try:
        from integrations.data_source import fetch_stock_hist

        rec_s = rec_date.strftime("%Y-%m-%d")
        market_norm = _normalize_market(market)
        hist = fetch_stock_hist(symbol, rec_s, rec_s, adjust="qfq", market=market_norm)
        if hist is not None and not hist.empty:
            close_s = pd.to_numeric(hist.get("收盘"), errors="coerce").dropna()
            if not close_s.empty:
                px = float(close_s.iloc[-1])
                if px > 0:
                    return px

        start_s = (rec_date - timedelta(days=7)).strftime("%Y-%m-%d")
        hist2 = fetch_stock_hist(symbol, start_s, rec_s, adjust="qfq", market=market_norm)
        if hist2 is None or hist2.empty:
            return 0.0
        df = hist2.copy()
        if "日期" not in df.columns or "收盘" not in df.columns:
            return 0.0
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
        df = df.dropna(subset=["日期", "收盘"]).sort_values("日期")
        if df.empty:
            return 0.0
        df = df[df["日期"].dt.date <= rec_date]
        if df.empty:
            return 0.0
        px = float(df["收盘"].iloc[-1])
        return px if px > 0 else 0.0
    except Exception:
        return 0.0

def upsert_recommendations(recommend_date: int, symbols_info: list[dict[str, Any]]) -> bool:
    """
    将每日选出的股票存入推荐跟踪表
    recommend_date: YYYYMMDD (int)
    """
    if not is_supabase_configured() or not symbols_info:
        return False
    try:
        client = _get_supabase_admin_client()
        schema_supports_market_symbol = _supports_market_symbol_schema(client)

        # 预读已有记录（按 market+symbol 聚合；旧表退化为 code 聚合），用于维护 recommend_count。
        # 规则：仅当 recommend_date 变化时才 +1，同日重跑不重复累计。
        existing_counts: dict[tuple[str, str], int] = {}
        existing_code_dates: dict[tuple[str, str], set[int]] = {}
        try:
            if schema_supports_market_symbol:
                resp = (
                    client.table(TABLE_RECOMMENDATION_TRACKING)
                    .select("market,symbol,code,recommend_count,recommend_date")
                    .execute()
                )
            else:
                resp = (
                    client.table(TABLE_RECOMMENDATION_TRACKING)
                    .select("code,recommend_count,recommend_date")
                    .execute()
                )
            for row in resp.data or []:
                market_key, symbol_key = _market_symbol_from_record(row)
                if not symbol_key:
                    continue
                try:
                    cnt = int(row.get("recommend_count") or 1)
                except Exception:
                    cnt = 1
                key = (market_key, symbol_key)
                existing_counts[key] = max(existing_counts.get(key, 0), cnt)
                try:
                    d = int(row.get("recommend_date"))
                    existing_code_dates.setdefault(key, set()).add(d)
                except Exception:
                    pass
        except Exception:
            existing_counts = {}
            existing_code_dates = {}

        payload = []
        for s in symbols_info:
            market = _normalize_market(s.get("market"), default="cn")
            symbol = _normalize_symbol(s.get("symbol") or s.get("code"), market=market)
            if not symbol:
                continue
            code_int = _legacy_code_from_symbol(symbol, market)
            
            # price 优先使用 step2 传入的 initial_price，并做多字段兜底
            price = 0.0
            for key in ("initial_price", "current_price", "price", "latest_price", "close"):
                raw_price = s.get(key)
                if raw_price is None or raw_price == "":
                    continue
                try:
                    parsed = float(raw_price)
                except Exception:
                    continue
                if parsed > 0:
                    price = parsed
                    break

            score_val: float | None = None
            for score_key in ("funnel_score", "priority_score", "score"):
                raw_score = s.get(score_key)
                if raw_score is None or raw_score == "":
                    continue
                try:
                    score_val = float(raw_score)
                    break
                except Exception:
                    continue
            
            key = (market, symbol)
            old_cnt = existing_counts.get(key, 0)
            seen_dates = existing_code_dates.get(key, set())
            if old_cnt <= 0:
                new_cnt = 1
            elif recommend_date in seen_dates:
                new_cnt = old_cnt
            else:
                new_cnt = old_cnt + 1

            payload.append({
                "market": market,
                "symbol": symbol,
                "code": code_int,
                "name": str(s.get("name", "")).strip(),
                "recommend_reason": str(s.get("tag", "")).strip(),
                "recommend_date": recommend_date,
                "initial_price": price,
                "current_price": price, # 初始时当前价等于加入价
                "change_pct": 0.0,      # 初始涨跌幅为 0
                "recommend_count": new_cnt,
                "funnel_score": score_val,
                "is_ai_recommended": False,
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
        
        if payload:
            optional_cols = ("is_ai_recommended", "funnel_score", "recommend_count")
            if schema_supports_market_symbol:
                try:
                    client.table(TABLE_RECOMMENDATION_TRACKING).upsert(
                        payload, on_conflict="market,symbol,recommend_date"
                    ).execute()
                except Exception as e:
                    msg = str(e).lower()
                    if any(col in msg for col in optional_cols):
                        fallback_payload: list[dict[str, Any]] = []
                        for row in payload:
                            r = dict(row)
                            for col in optional_cols:
                                r.pop(col, None)
                            fallback_payload.append(r)
                        client.table(TABLE_RECOMMENDATION_TRACKING).upsert(
                            fallback_payload, on_conflict="market,symbol,recommend_date"
                        ).execute()
                    else:
                        raise
            else:
                legacy_payload: list[dict[str, Any]] = []
                for row in payload:
                    if row.get("market") != "cn" or row.get("code") is None:
                        print(
                            "[supabase_recommendation] upsert_recommendations skipped: "
                            "table missing market/symbol columns; apply SQL migration before writing US rows"
                        )
                        return False
                    legacy_payload.append(
                        {
                            "code": row.get("code"),
                            "name": row.get("name"),
                            "recommend_reason": row.get("recommend_reason"),
                            "recommend_date": row.get("recommend_date"),
                            "initial_price": row.get("initial_price"),
                            "current_price": row.get("current_price"),
                            "change_pct": row.get("change_pct"),
                            "recommend_count": row.get("recommend_count"),
                            "funnel_score": row.get("funnel_score"),
                            "is_ai_recommended": row.get("is_ai_recommended"),
                            "updated_at": row.get("updated_at"),
                        }
                    )
                try:
                    client.table(TABLE_RECOMMENDATION_TRACKING).upsert(
                        legacy_payload, on_conflict="code,recommend_date"
                    ).execute()
                except Exception as e:
                    msg = str(e).lower()
                    if any(col in msg for col in optional_cols):
                        fallback_payload: list[dict[str, Any]] = []
                        for row in legacy_payload:
                            r = dict(row)
                            for col in optional_cols:
                                r.pop(col, None)
                            fallback_payload.append(r)
                        client.table(TABLE_RECOMMENDATION_TRACKING).upsert(
                            fallback_payload, on_conflict="code,recommend_date"
                        ).execute()
                    else:
                        raise
        return True
    except Exception as e:
        print(f"[supabase_recommendation] upsert_recommendations failed: {e}")
        return False


def mark_ai_recommendations(
    recommend_date: int,
    ai_codes: list[str],
    *,
    market: str = "cn",
) -> bool:
    """
    将某个推荐日的记录标记为是否 AI 推荐（可操作池）。
    ai_codes 传入 6 位代码字符串列表。
    """
    if not is_supabase_configured():
        return False
    try:
        client = _get_supabase_admin_client()
        schema_supports_market_symbol = _supports_market_symbol_schema(client)
        now_iso = datetime.now(timezone.utc).isoformat()
        market_norm = _normalize_market(market)
        # 先全量置 false，再对白名单置 true，避免前一次残留。
        if schema_supports_market_symbol:
            client.table(TABLE_RECOMMENDATION_TRACKING).update(
                {"is_ai_recommended": False, "updated_at": now_iso}
            ).eq("recommend_date", recommend_date).eq("market", market_norm).execute()

            symbols = sorted(
                {
                    symbol
                    for symbol in (
                        _normalize_symbol(code, market=market_norm) for code in (ai_codes or [])
                    )
                    if symbol
                }
            )
            if symbols:
                client.table(TABLE_RECOMMENDATION_TRACKING).update(
                    {"is_ai_recommended": True, "updated_at": now_iso}
                ).eq("recommend_date", recommend_date).eq("market", market_norm).in_("symbol", symbols).execute()
        else:
            if market_norm != "cn":
                print(
                    "[supabase_recommendation] mark_ai_recommendations skipped: "
                    "table missing market/symbol columns; apply SQL migration before marking US rows"
                )
                return False
            client.table(TABLE_RECOMMENDATION_TRACKING).update(
                {"is_ai_recommended": False, "updated_at": now_iso}
            ).eq("recommend_date", recommend_date).execute()

            code_ints: list[int] = []
            for code in ai_codes or []:
                normalized = _normalize_symbol(code, market="cn")
                if not normalized:
                    continue
                try:
                    code_ints.append(int(normalized))
                except Exception:
                    continue
            code_ints = sorted(set(code_ints))
            if code_ints:
                client.table(TABLE_RECOMMENDATION_TRACKING).update(
                    {"is_ai_recommended": True, "updated_at": now_iso}
                ).eq("recommend_date", recommend_date).in_("code", code_ints).execute()
        return True
    except Exception as e:
        msg = str(e)
        if "is_ai_recommended" in msg:
            print(
                "[supabase_recommendation] mark_ai_recommendations skipped: "
                "missing column is_ai_recommended (please run SQL migration)"
            )
            return False
        print(f"[supabase_recommendation] mark_ai_recommendations failed: {e}")
        return False

def sync_all_tracking_prices(
    price_map: dict[str, float] | None = None,
) -> int:
    """
    遍历表中所有股票，用最新价刷新 current_price 与 change_pct。
    price_map: 可选，code_str -> 最新收盘价。非空时优先使用；
    对缺失代码优先回退到历史日线收盘（qfq），最后才按开关尝试实时快照。
    返回成功更新的数量。
    """
    if not is_supabase_configured():
        print("[supabase_recommendation] sync_all_tracking_prices: Supabase 未配置，跳过")
        return 0

    try:
        client = _get_supabase_admin_client()
        schema_supports_market_symbol = _supports_market_symbol_schema(client)
        allow_spot_fallback = (
            os.getenv("RECOMMENDATION_PRICE_ALLOW_SPOT_FALLBACK", "").strip().lower()
            in {"1", "true", "yes", "on"}
        )

        resp = client.table(TABLE_RECOMMENDATION_TRACKING).select("*").execute()
        if not resp.data:
            print("[supabase_recommendation] sync_all_tracking_prices: 推荐表无记录，跳过")
            return 0

        grouped_records: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in resp.data:
            if not isinstance(row, dict):
                continue
            market, symbol = _market_symbol_from_record(row)
            if not symbol:
                continue
            grouped_records.setdefault((market, symbol), []).append(row)
        if not grouped_records:
            print("[supabase_recommendation] sync_all_tracking_prices: 推荐表无有效 symbol，跳过")
            return 0

        # 统一日线窗口（与 step2 同口径），避免实时快照不稳定导致脏数据。
        hist_window_cache: dict[str, tuple[str, str]] = {}
        hist_close_cache: dict[tuple[str, str], float] = {}

        def _resolve_hist_window(market: str) -> tuple[str | None, str | None]:
            if market in hist_window_cache:
                return hist_window_cache[market]
            try:
                from integrations.fetch_a_share_csv import _resolve_trading_window, _resolve_us_window
                from utils.trading_clock import resolve_end_calendar_day

                if market == "us":
                    window = _resolve_us_window(
                        end_calendar_day=resolve_end_calendar_day(),
                        trading_days=20,
                    )
                else:
                    window = _resolve_trading_window(
                        end_calendar_day=resolve_end_calendar_day(),
                        trading_days=20,
                    )
                value = (
                    window.start_trade_date.strftime("%Y-%m-%d"),
                    window.end_trade_date.strftime("%Y-%m-%d"),
                )
            except Exception:
                value = (None, None)
            hist_window_cache[market] = value
            return value

        def _price_from_history(symbol: str, market: str) -> float | None:
            cache_key = (market, symbol)
            if cache_key in hist_close_cache:
                cached = hist_close_cache[cache_key]
                return cached if cached > 0 else None
            hist_start_s, hist_end_s = _resolve_hist_window(market)
            if not hist_start_s or not hist_end_s:
                hist_close_cache[cache_key] = 0.0
                return None
            try:
                from integrations.data_source import fetch_stock_hist

                hist = fetch_stock_hist(
                    symbol,
                    hist_start_s,
                    hist_end_s,
                    adjust="qfq",
                    market=market,
                )
                if hist is None or hist.empty or "收盘" not in hist.columns:
                    hist_close_cache[cache_key] = 0.0
                    return None
                close_s = pd.to_numeric(hist.get("收盘"), errors="coerce").dropna()
                if close_s.empty:
                    hist_close_cache[cache_key] = 0.0
                    return None
                px = float(close_s.iloc[-1])
                hist_close_cache[cache_key] = px if px > 0 else 0.0
                return px if px > 0 else None
            except Exception:
                hist_close_cache[cache_key] = 0.0
                return None

        def _price_from_spot(symbol: str, market: str) -> float | None:
            if not allow_spot_fallback or market != "cn":
                return None
            try:
                from integrations.data_source import fetch_stock_spot_snapshot

                snap = fetch_stock_spot_snapshot(symbol, force_refresh=False)
                if not snap or snap.get("close") is None:
                    return None
                px = float(snap["close"])
                return px if px > 0 else None
            except Exception:
                return None

        updated_count = 0
        for (market, symbol), records in grouped_records.items():
            new_current_price: float | None = None

            if price_map:
                raw_px = price_map.get(f"{market}:{symbol}")
                if raw_px is None:
                    raw_px = price_map.get(symbol)
                try:
                    parsed_px = float(raw_px) if raw_px is not None else 0.0
                except Exception:
                    parsed_px = 0.0
                if parsed_px > 0:
                    new_current_price = parsed_px

            if new_current_price is None:
                new_current_price = _price_from_history(symbol, market)
            if new_current_price is None:
                new_current_price = _price_from_spot(symbol, market)
            if new_current_price is None:
                continue
            
            # 该股票可能有多条推荐记录（不同日期），逐条更新价格与涨跌幅
            for record in records:
                initial_price = float(record.get("initial_price") or 0.0)
                rec_date = _parse_recommend_date(record.get("recommend_date"))
                update_payload = {
                    "current_price": new_current_price,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                if initial_price > 0:
                    change_pct = (new_current_price - initial_price) / initial_price * 100.0
                    update_payload["change_pct"] = round(change_pct, 2)
                else:
                    backfill_price = (
                        _resolve_initial_price_from_history(symbol, rec_date, market=market)
                        if rec_date
                        else 0.0
                    )
                    if backfill_price <= 0:
                        backfill_price = new_current_price
                    update_payload["initial_price"] = backfill_price
                    update_payload["change_pct"] = (
                        round(
                            (new_current_price - backfill_price) / backfill_price * 100.0,
                            2,
                        )
                        if backfill_price > 0
                        else 0.0
                    )
                client.table(TABLE_RECOMMENDATION_TRACKING).update(update_payload).eq("id", record["id"]).execute()
                updated_count += 1

        if grouped_records and updated_count == 0:
            print(
                "[supabase_recommendation] sync_all_tracking_prices: 推荐表有 {} 只股票但 0 条更新，"
                "可能是 price_map 为空且历史/实时行情均不可用".format(len(grouped_records))
            )
        return updated_count
    except Exception as e:
        print(f"[supabase_recommendation] sync_all_tracking_prices failed: {e}")
        return 0


def correct_tracking_initial_prices() -> int:
    """
    纠错流程：遍历推荐表每条记录，用「推荐日」当天收盘价（前复权）回填 initial_price，
    并用当前 current_price 重算 change_pct。
    每日执行可让历史数据逐步修正。
    返回被更新的记录数。
    """
    if not is_supabase_configured():
        print("[supabase_recommendation] correct_tracking_initial_prices: Supabase 未配置，跳过")
        return 0
    try:
        client = _get_supabase_admin_client()
        resp = client.table(TABLE_RECOMMENDATION_TRACKING).select("*").execute()
        if not resp.data:
            return 0
        cache: dict[tuple[str, str, date], float] = {}
        updated = 0
        for record in resp.data:
            write_date = _parse_write_date(record)
            if not write_date:
                continue
            market, symbol = _market_symbol_from_record(record)
            if not symbol:
                continue
            current_price = float(record.get("current_price") or 0.0)
            if current_price <= 0:
                continue
            key = (market, symbol, write_date)
            if key not in cache:
                cache[key] = _resolve_initial_price_from_history(symbol, write_date, market=market)
            initial_from_hist = cache[key]
            if initial_from_hist <= 0:
                continue
            change_pct = round((current_price - initial_from_hist) / initial_from_hist * 100.0, 2)
            client.table(TABLE_RECOMMENDATION_TRACKING).update({
                "initial_price": initial_from_hist,
                "change_pct": change_pct,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", record["id"]).execute()
            updated += 1
        return updated
    except Exception as e:
        print(f"[supabase_recommendation] correct_tracking_initial_prices failed: {e}")
        return 0


def load_recommendation_tracking(limit: int = 1000) -> list[dict[str, Any]]:
    """加载推荐跟踪数据"""
    try:
        client = _get_supabase_admin_client()
        resp = (
            client.table(TABLE_RECOMMENDATION_TRACKING)
            .select("*")
            .order("recommend_date", desc=True)
            .limit(limit)
            .execute()
        )
        rows: list[dict[str, Any]] = []
        for row in resp.data or []:
            if not isinstance(row, dict):
                continue
            market, symbol = _market_symbol_from_record(row)
            if not symbol:
                continue
            normalized = dict(row)
            normalized["market"] = market
            normalized["symbol"] = symbol
            if normalized.get("code") is None:
                legacy_code = _legacy_code_from_symbol(symbol, market)
                if legacy_code is not None:
                    normalized["code"] = legacy_code
            rows.append(normalized)
        return rows
    except Exception as e:
        print(f"[supabase_recommendation] load_recommendation_tracking failed: {e}")
        return []


def _to_ts_code_recommendation(symbol: str) -> str:
    s = "".join(ch for ch in str(symbol or "") if ch.isdigit())
    s = s[-6:].zfill(6)
    if s.startswith(("600", "601", "603", "605", "688")):
        return f"{s}.SH"
    return f"{s}.SZ"


def _recommend_date_to_yyyymmdd(raw: Any) -> str:
    d = _parse_recommend_date(raw)
    if d is None:
        return ""
    return d.strftime("%Y%m%d")


def _pick_close_on_or_before(sorted_trade_dates: list[str], target_yyyymmdd: str) -> str:
    if not sorted_trade_dates or not target_yyyymmdd:
        return ""
    i = bisect_right(sorted_trade_dates, target_yyyymmdd) - 1
    if i < 0:
        return ""
    return sorted_trade_dates[i]


def refresh_tracking_prices_with_hist_data(
    *,
    market: str = "",
) -> dict[str, Any]:
    """
    使用统一历史行情回填并刷新推荐跟踪价格：
    - initial_price: 推荐日（或之前最近交易日）收盘价
    - current_price: 当前系统时间对应最近交易日收盘价
    - change_pct: (current - initial) / initial * 100
    """
    from integrations.data_source import fetch_stock_hist

    if not is_supabase_configured():
        raise ValueError("SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY 未配置")

    client = _get_supabase_admin_client()
    schema_supports_market_symbol = _supports_market_symbol_schema(client)
    market_filter = str(market or "").strip().lower()
    if market_filter not in {"", "cn", "us"}:
        raise ValueError("market must be '', 'cn', or 'us'")
    resp = (
        client.table(TABLE_RECOMMENDATION_TRACKING)
        .select("id,market,symbol,code,recommend_date")
        .execute()
    )
    records = resp.data or []
    if not records:
        return {
            "rows_total": 0,
            "rows_updated": 0,
            "rows_skipped": 0,
            "codes_total": 0,
            "codes_no_data": 0,
            "latest_trade_date": "",
        }

    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
    end_date = today.strftime("%Y-%m-%d")

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in records:
        market, symbol = _market_symbol_from_record(row)
        if market_filter and market != market_filter:
            continue
        if not symbol:
            continue
        grouped.setdefault((market, symbol), []).append({**row, "market": market, "symbol": symbol})

    updates: list[dict[str, Any]] = []
    codes_no_data = 0
    latest_trade_date_global = ""

    for (market, symbol), rows in grouped.items():
        rec_dates = [
            _recommend_date_to_yyyymmdd(r.get("recommend_date"))
            for r in rows
        ]
        rec_dates = [d for d in rec_dates if d]
        if not rec_dates:
            continue
        start_date = (
            datetime.strptime(min(rec_dates), "%Y%m%d").date() - timedelta(days=7)
        ).strftime("%Y-%m-%d")

        try:
            hist = fetch_stock_hist(
                symbol,
                start_date,
                end_date,
                adjust="qfq",
                market=market,
            )
        except Exception as e:
            print(f"[supabase_recommendation] history refresh failed {market}:{symbol}: {e}")
            codes_no_data += 1
            continue

        close_map = _extract_close_map_from_hist(hist)
        if not close_map:
            codes_no_data += 1
            continue

        trade_dates = sorted(close_map.keys())
        current_trade_date = trade_dates[-1]
        current_close = float(close_map[current_trade_date])
        if not latest_trade_date_global or current_trade_date > latest_trade_date_global:
            latest_trade_date_global = current_trade_date

        for row in rows:
            rec_date = _recommend_date_to_yyyymmdd(row.get("recommend_date"))
            pick_date = _pick_close_on_or_before(trade_dates, rec_date)
            if not pick_date:
                continue
            initial_close = float(close_map[pick_date])
            if initial_close <= 0 or current_close <= 0:
                continue
            change_pct = round((current_close - initial_close) / initial_close * 100.0, 2)
            row_id = row.get("id")
            update_payload = {
                "id": row_id,
                "initial_price": round(initial_close, 4),
                "current_price": round(current_close, 4),
                "change_pct": change_pct,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if schema_supports_market_symbol:
                update_payload["market"] = market
                update_payload["symbol"] = symbol
            else:
                legacy_code = _legacy_code_from_symbol(symbol, market)
                if legacy_code is None:
                    continue
                update_payload["code"] = legacy_code
                update_payload["recommend_date"] = int(rec_date) if rec_date.isdigit() else None
            updates.append(
                update_payload
            )

    if updates:
        for item in updates:
            row_id = item.pop("id", None)
            market_val = item.pop("market", None)
            symbol_val = item.pop("symbol", None)
            code_val = item.pop("code", None)
            rec_date_val = item.pop("recommend_date", None)
            q = client.table(TABLE_RECOMMENDATION_TRACKING).update(item)
            if row_id is not None:
                q = q.eq("id", row_id)
            elif schema_supports_market_symbol and market_val and symbol_val and rec_date_val is not None:
                q = q.eq("market", market_val).eq("symbol", symbol_val).eq("recommend_date", rec_date_val)
            elif code_val is not None and rec_date_val is not None:
                q = q.eq("code", code_val).eq("recommend_date", rec_date_val)
            else:
                continue
            q.execute()

    updated_keys = {
        f"{x.get('market', '')}:{x.get('symbol', x.get('code', ''))}:{x.get('recommend_date', '')}"
        for x in updates
        if x.get("recommend_date") is not None
    }
    return {
        "rows_total": len(records),
        "rows_updated": len(updated_keys),
        "rows_skipped": max(len(records) - len(updated_keys), 0),
        "codes_total": len(grouped),
        "codes_no_data": codes_no_data,
        "latest_trade_date": latest_trade_date_global,
    }


def refresh_tracking_prices_with_tushare_unadjusted(
    *,
    market: str = "",
) -> dict[str, Any]:
    """兼容旧调用名，内部已切换到统一历史行情入口。"""
    return refresh_tracking_prices_with_hist_data(market=market)
