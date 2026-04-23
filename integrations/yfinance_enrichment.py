from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SHARES_CACHE_PATH = _DATA_DIR / "yfinance_shares_cache.json"
_ENRICH_CACHE_PATH = _DATA_DIR / "yfinance_candidate_enrichment_cache.json"

_SHARES_CACHE_TTL = int(os.getenv("YFINANCE_SHARES_CACHE_TTL_SECONDS", str(7 * 24 * 60 * 60)))
_ENRICH_CACHE_TTL = int(os.getenv("YFINANCE_ENRICH_CACHE_TTL_SECONDS", str(12 * 60 * 60)))
_SHARES_REFRESH_MAX_PER_RUN = max(int(os.getenv("YFINANCE_SHARES_REFRESH_MAX_PER_RUN", "80")), 0)
_CANDIDATE_ENRICH_MAX_SYMBOLS = max(int(os.getenv("YFINANCE_CANDIDATE_ENRICH_MAX_SYMBOLS", "25")), 0)


def _now_ts() -> float:
    return time.time()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json_dump(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(_jsonable(payload), tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            tmp_name = tmp.name
        Path(tmp_name).replace(path)
        tmp_name = None
    finally:
        if tmp_name:
            try:
                Path(tmp_name).unlink(missing_ok=True)
            except Exception:
                pass


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _currency_to_cny_rate(currency: str) -> float:
    cur = str(currency or "").strip().upper()
    if cur in {"CNY", "CNH", "RMB"}:
        return 1.0
    if cur == "HKD":
        try:
            return float(os.getenv("HKD_CNY_RATE", "0.92"))
        except Exception:
            return 0.92
    if cur == "USD":
        try:
            return float(os.getenv("USD_CNY_RATE", "7.20"))
        except Exception:
            return 7.20
    try:
        return float(os.getenv(f"{cur}_CNY_RATE", "1.0"))
    except Exception:
        return 1.0


def normalize_yfinance_symbol(symbol: str, market: str) -> str:
    market_norm = str(market or "cn").strip().lower()
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if market_norm == "cn":
        if text.endswith((".SS", ".SZ")):
            return text
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) != 6:
            return ""
        if digits.startswith(("600", "601", "603", "605", "688")):
            return f"{digits}.SS"
        if digits.startswith(("000", "001", "002", "003", "300", "301")):
            return f"{digits}.SZ"
        return ""
    if market_norm == "hk":
        if text.endswith(".HK"):
            return text
        digits = "".join(ch for ch in text if ch.isdigit())
        return f"{digits[-4:].zfill(4)}.HK" if digits else ""
    if market_norm == "us":
        return text.replace(".", "-").replace("/", "-")
    return text


def _map_key(symbol: str, market: str) -> str:
    market_norm = str(market or "cn").strip().lower()
    text = str(symbol or "").strip().upper()
    if market_norm == "cn":
        return text.split(".")[0]
    if market_norm == "hk":
        return normalize_yfinance_symbol(text, "hk")
    if market_norm == "us":
        return normalize_yfinance_symbol(text, "us")
    return text


def _latest_close_from_frame(df: pd.DataFrame | None) -> float | None:
    if df is None or df.empty or "close" not in df.columns:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if close.empty:
        return None
    value = float(close.iloc[-1])
    return value if value > 0 else None


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except Exception:
        return None


def _fast_value(fast_info: Any, *names: str) -> Any:
    for name in names:
        try:
            value = getattr(fast_info, name)
            if value is not None:
                return value
        except Exception:
            pass
        try:
            value = fast_info[name]
            if value is not None:
                return value
        except Exception:
            pass
        try:
            value = fast_info.get(name)
            if value is not None:
                return value
        except Exception:
            pass
    return None


def _is_fresh(row: dict[str, Any], ttl_seconds: int) -> bool:
    try:
        updated_ts = float(row.get("updated_ts") or 0)
    except Exception:
        updated_ts = 0.0
    return updated_ts > 0 and (_now_ts() - updated_ts) < ttl_seconds


def _load_shares_items() -> dict[str, dict[str, Any]]:
    payload = _load_json(_SHARES_CACHE_PATH)
    items = payload.get("items", {}) if isinstance(payload, dict) else {}
    return items if isinstance(items, dict) else {}


def _save_shares_items(items: dict[str, dict[str, Any]]) -> None:
    _atomic_json_dump(
        _SHARES_CACHE_PATH,
        {
            "source": "yfinance_shares",
            "updated_at": _now_iso(),
            "items": items,
        },
    )


def _fetch_share_record(yf: Any, yf_symbol: str, market: str) -> dict[str, Any]:
    ticker = yf.Ticker(yf_symbol)
    fast = None
    info: dict[str, Any] = {}
    try:
        fast = ticker.fast_info
    except Exception:
        fast = None
    shares = _to_float(_fast_value(fast, "shares", "sharesOutstanding"))
    currency = str(
        _fast_value(fast, "currency")
        or ("USD" if market == "us" else "HKD" if market == "hk" else "CNY")
    ).strip().upper()
    fetch_info = (
        shares is None
        or not currency
        or os.getenv("YFINANCE_SHARES_FETCH_INFO", "0").strip().lower() in {"1", "true", "yes", "on"}
    )
    if fetch_info:
        try:
            raw_info = ticker.info
            if isinstance(raw_info, dict):
                info = raw_info
        except Exception:
            info = {}
    if shares is None:
        shares = _to_float(info.get("sharesOutstanding"))
    float_shares = _to_float(info.get("floatShares"))
    if info.get("currency"):
        currency = str(info.get("currency") or currency).strip().upper()
    info_market_cap = _to_float(info.get("marketCap"))
    fast_market_cap = _to_float(_fast_value(fast, "market_cap", "marketCap"))

    return {
        "symbol": yf_symbol,
        "market": market,
        "shares_outstanding": shares,
        "float_shares": float_shares,
        "currency": currency,
        "info_market_cap": info_market_cap,
        "fast_market_cap": fast_market_cap,
        "source": "yfinance.fast_info+info",
        "updated_at": _now_iso(),
        "updated_ts": _now_ts(),
    }


def build_market_cap_map_from_shares(
    *,
    symbols: list[str],
    market: str,
    df_map: dict[str, pd.DataFrame],
    base_map: dict[str, float] | None = None,
    refresh_missing: bool = True,
) -> tuple[dict[str, float], dict[str, Any]]:
    """
    Build CNY-equivalent market cap map from cached shares and latest daily close.

    Missing share rows are refreshed up to YFINANCE_SHARES_REFRESH_MAX_PER_RUN to
    avoid turning the daily OHLCV fetch into thousands of Yahoo metadata calls.
    """
    market_norm = str(market or "cn").strip().lower()
    if market_norm not in {"cn", "us", "hk"}:
        market_norm = "cn"
    out: dict[str, float] = {
        str(k).strip().upper() if market_norm != "cn" else str(k).strip().split(".")[0]: float(v)
        for k, v in (base_map or {}).items()
        if _to_float(v) is not None and float(v) > 0
    }
    cache = _load_shares_items()
    refresh_budget = _SHARES_REFRESH_MAX_PER_RUN if refresh_missing else 0
    refreshed = 0
    computed = 0
    missing_shares = 0
    missing_close = 0

    try:
        import yfinance as yf
    except Exception:
        yf = None

    changed = False
    for raw_symbol in symbols:
        key = _map_key(raw_symbol, market_norm)
        yf_symbol = normalize_yfinance_symbol(raw_symbol, market_norm)
        if not key or not yf_symbol:
            continue
        row = cache.get(yf_symbol)
        if refresh_missing and yf is not None and (not isinstance(row, dict) or not _is_fresh(row, _SHARES_CACHE_TTL)):
            if refreshed < refresh_budget:
                try:
                    row = _fetch_share_record(yf, yf_symbol, market_norm)
                    cache[yf_symbol] = row
                    changed = True
                    refreshed += 1
                except Exception as exc:
                    cache[yf_symbol] = {
                        "symbol": yf_symbol,
                        "market": market_norm,
                        "error": f"{type(exc).__name__}: {exc}",
                        "updated_at": _now_iso(),
                        "updated_ts": _now_ts(),
                    }
                    row = cache[yf_symbol]
                    changed = True
                    refreshed += 1
        if not isinstance(row, dict):
            missing_shares += 1
            continue
        shares = _to_float(row.get("shares_outstanding"))
        if shares is None or shares <= 0:
            missing_shares += 1
            continue
        df = df_map.get(raw_symbol)
        if df is None:
            df = df_map.get(key)
        if df is None:
            df = df_map.get(yf_symbol)
        close = _latest_close_from_frame(df)
        if close is None:
            missing_close += 1
            continue
        currency = str(row.get("currency") or "").strip().upper()
        market_cap_yi = shares * close * _currency_to_cny_rate(currency) / 1e8
        if market_cap_yi > 0:
            out[key] = float(market_cap_yi)
            computed += 1

    if changed:
        _save_shares_items(cache)
    return out, {
        "source": "shares_outstanding*daily_close",
        "computed": computed,
        "refreshed": refreshed,
        "missing_shares": missing_shares,
        "missing_close": missing_close,
        "cache_path": str(_SHARES_CACHE_PATH),
        "refresh_budget": refresh_budget,
        "base_count": len(base_map or {}),
        "total": len(out),
    }


def _load_enrich_items() -> dict[str, dict[str, Any]]:
    payload = _load_json(_ENRICH_CACHE_PATH)
    items = payload.get("items", {}) if isinstance(payload, dict) else {}
    return items if isinstance(items, dict) else {}


def _save_enrich_items(items: dict[str, dict[str, Any]]) -> None:
    _atomic_json_dump(
        _ENRICH_CACHE_PATH,
        {
            "source": "yfinance_candidate_enrichment",
            "updated_at": _now_iso(),
            "items": items,
        },
    )


def _frame_records(df: Any, limit: int = 6) -> list[dict[str, Any]]:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return []
    work = df.reset_index()
    out: list[dict[str, Any]] = []
    for _, row in work.head(limit).iterrows():
        rec: dict[str, Any] = {}
        for col, value in row.items():
            if isinstance(value, (pd.Timestamp, datetime, date)):
                rec[str(col)] = value.isoformat()
            elif not isinstance(value, (dict, list, tuple, set)) and pd.isna(value):
                rec[str(col)] = None
            else:
                rec[str(col)] = _jsonable(value)
        out.append(rec)
    return out


def _latest_period_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return rows[0] if rows else {}


def _safe_pct(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return (numerator - denominator) / abs(denominator) * 100.0


def _score_revision(eps_rows: list[dict[str, Any]], rev_rows: list[dict[str, Any]]) -> float:
    score = 0.0
    latest = _latest_period_row(eps_rows)
    current = _to_float(latest.get("current"))
    for key, weight in [("7daysAgo", 1.0), ("30daysAgo", 1.5), ("60daysAgo", 1.0), ("90daysAgo", 0.5)]:
        old = _to_float(latest.get(key))
        pct = _safe_pct(current, old)
        if pct is None:
            continue
        if pct > 0:
            score += min(pct / 5.0, 2.0) * weight
        elif pct < 0:
            score -= min(abs(pct) / 5.0, 2.0) * weight
    latest_rev = _latest_period_row(rev_rows)
    up = (_to_float(latest_rev.get("upLast7days")) or 0) + (_to_float(latest_rev.get("upLast30days")) or 0)
    down = (_to_float(latest_rev.get("downLast7Days")) or 0) + (_to_float(latest_rev.get("downLast30days")) or 0)
    score += min(up - down, 5.0)
    return round(max(min(score, 10.0), -10.0), 2)


def _score_surprise(history_rows: list[dict[str, Any]], date_rows: list[dict[str, Any]]) -> float:
    surprises: list[float] = []
    for row in history_rows + date_rows:
        for key in ("surprisePercent", "Surprise(%)"):
            value = _to_float(row.get(key))
            if value is not None:
                surprises.append(value)
                break
        if len(surprises) >= 4:
            break
    if not surprises:
        return 0.0
    avg = sum(surprises) / len(surprises)
    positive_ratio = sum(1 for x in surprises if x > 0) / len(surprises)
    score = avg * 10.0 + positive_ratio * 4.0
    return round(max(min(score, 10.0), -10.0), 2)


def _future_earnings_event(calendar: dict[str, Any], date_rows: list[dict[str, Any]]) -> dict[str, Any]:
    today = date.today()
    candidates: list[date] = []
    raw_dates = calendar.get("Earnings Date") if isinstance(calendar, dict) else None
    if isinstance(raw_dates, list):
        for item in raw_dates:
            try:
                candidates.append(pd.to_datetime(item).date())
            except Exception:
                pass
    for row in date_rows:
        raw = row.get("Earnings Date") or row.get("index")
        try:
            d = pd.to_datetime(raw).date()
        except Exception:
            continue
        if d >= today:
            candidates.append(d)
    if not candidates:
        return {"has_event": False, "days_to_event": None, "date": None}
    next_date = min(candidates)
    days = (next_date - today).days
    return {
        "has_event": 0 <= days <= 14,
        "days_to_event": days,
        "date": next_date.isoformat(),
    }


def _news_items(raw_news: Any, limit: int = 5) -> list[dict[str, Any]]:
    if not isinstance(raw_news, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw_news[:limit]:
        content = item.get("content", item) if isinstance(item, dict) else {}
        if not isinstance(content, dict):
            continue
        provider = content.get("provider") if isinstance(content.get("provider"), dict) else {}
        url = content.get("canonicalUrl") if isinstance(content.get("canonicalUrl"), dict) else {}
        out.append(
            {
                "title": str(content.get("title") or "").strip(),
                "summary": str(content.get("summary") or content.get("description") or "").strip(),
                "pubDate": str(content.get("pubDate") or content.get("displayTime") or "").strip(),
                "provider": str(provider.get("displayName") or "").strip(),
                "url": str(url.get("url") or "").strip(),
            }
        )
    return [x for x in out if x.get("title")]


def _format_enrichment_context(row: dict[str, Any]) -> str:
    parts = []
    factors = row.get("factors", {}) if isinstance(row.get("factors"), dict) else {}
    target_gap = factors.get("target_price_gap_pct")
    event = factors.get("earnings_event_risk", {}) if isinstance(factors.get("earnings_event_risk"), dict) else {}
    parts.append(
        "[基本面/预期因子] "
        f"预期上修={factors.get('estimate_revision_score', 0):+.1f}, "
        f"收益Surprise={factors.get('earnings_surprise_score', 0):+.1f}, "
        f"目标价偏离={(f'{target_gap:+.1f}%' if target_gap is not None else '-')}, "
        f"财报事件={('未来' + str(event.get('days_to_event')) + '天' if event.get('has_event') else '无近14天事件')}"
    )
    news = row.get("news", []) if isinstance(row.get("news"), list) else []
    if news:
        brief = "；".join(
            f"{n.get('pubDate', '')[:10]} {n.get('title', '')}"
            for n in news[:3]
            if n.get("title")
        )
        if brief:
            parts.append(f"[相关新闻] {brief}")
    return "\n  ".join(parts)


def _fetch_candidate_enrichment(yf: Any, yf_symbol: str, market: str) -> dict[str, Any]:
    ticker = yf.Ticker(yf_symbol)
    fast = ticker.fast_info
    current_price = _to_float(_fast_value(fast, "last_price", "lastPrice"))
    analyst_targets = ticker.analyst_price_targets
    if not isinstance(analyst_targets, dict):
        analyst_targets = {}
    mean_target = _to_float(analyst_targets.get("mean"))
    if mean_target is None:
        mean_target = _to_float(analyst_targets.get("average"))
    eps_trend_rows = _frame_records(ticker.eps_trend)
    eps_revision_rows = _frame_records(ticker.eps_revisions)
    earnings_history_rows = _frame_records(ticker.earnings_history)
    earnings_dates_rows = _frame_records(ticker.get_earnings_dates(limit=8))
    earnings_estimate_rows = _frame_records(ticker.earnings_estimate)
    revenue_estimate_rows = _frame_records(ticker.revenue_estimate)
    calendar = ticker.calendar if isinstance(ticker.calendar, dict) else {}
    news = _news_items(ticker.news)

    target_gap = None
    if current_price and mean_target:
        target_gap = round((mean_target - current_price) / current_price * 100.0, 2)
    event = _future_earnings_event(calendar, earnings_dates_rows)
    factors = {
        "estimate_revision_score": _score_revision(eps_trend_rows, eps_revision_rows),
        "earnings_surprise_score": _score_surprise(earnings_history_rows, earnings_dates_rows),
        "target_price_gap_pct": target_gap,
        "earnings_event_risk": event,
    }
    row = {
        "symbol": yf_symbol,
        "market": market,
        "current_price": current_price,
        "analyst_price_targets": analyst_targets,
        "factors": factors,
        "samples": {
            "earnings_estimate": earnings_estimate_rows[:2],
            "revenue_estimate": revenue_estimate_rows[:2],
            "earnings_history": earnings_history_rows[:4],
            "eps_trend": eps_trend_rows[:2],
            "eps_revisions": eps_revision_rows[:2],
            "earnings_dates": earnings_dates_rows[:4],
        },
        "news": news,
        "source": "yfinance.candidate_interfaces",
        "updated_at": _now_iso(),
        "updated_ts": _now_ts(),
    }
    row["context"] = _format_enrichment_context(row)
    return row


def enrich_candidates(
    items: list[dict[str, Any]],
    *,
    market: str,
    force: bool = False,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if os.getenv("YFINANCE_CANDIDATE_ENRICH_ENABLED", "1").strip().lower() not in {"1", "true", "yes", "on"}:
        return {}, {"enabled": False, "reason": "YFINANCE_CANDIDATE_ENRICH_ENABLED=0"}
    market_norm = str(market or "cn").strip().lower()
    if market_norm not in {"cn", "us", "hk"}:
        market_norm = "cn"
    try:
        import yfinance as yf
    except Exception as exc:
        return {}, {"enabled": False, "reason": f"yfinance_unavailable: {exc}"}

    cache = _load_enrich_items()
    changed = False
    out: dict[str, dict[str, Any]] = {}
    fetched = 0
    cache_hits = 0
    errors = 0
    max_symbols = _CANDIDATE_ENRICH_MAX_SYMBOLS or len(items)

    for item in items[:max_symbols]:
        if not isinstance(item, dict):
            continue
        raw_symbol = str(item.get("code") or item.get("symbol") or "").strip()
        item_market = str(item.get("market") or market_norm).strip().lower()
        if item_market not in {"cn", "us", "hk"}:
            item_market = market_norm
        yf_symbol = normalize_yfinance_symbol(raw_symbol, item_market)
        key = _map_key(raw_symbol, item_market)
        if not yf_symbol or not key:
            continue
        row = cache.get(yf_symbol)
        if not force and isinstance(row, dict) and _is_fresh(row, _ENRICH_CACHE_TTL):
            cache_hits += 1
        else:
            try:
                row = _fetch_candidate_enrichment(yf, yf_symbol, item_market)
                cache[yf_symbol] = row
                changed = True
                fetched += 1
            except Exception as exc:
                row = {
                    "symbol": yf_symbol,
                    "market": item_market,
                    "error": f"{type(exc).__name__}: {exc}",
                    "context": "",
                    "updated_at": _now_iso(),
                    "updated_ts": _now_ts(),
                }
                cache[yf_symbol] = row
                changed = True
                fetched += 1
                errors += 1
        if isinstance(row, dict):
            out[key] = row

    if changed:
        _save_enrich_items(cache)
    return out, {
        "enabled": True,
        "requested": len(items),
        "processed": min(len(items), max_symbols),
        "returned": len(out),
        "fetched": fetched,
        "cache_hits": cache_hits,
        "errors": errors,
        "cache_path": str(_ENRICH_CACHE_PATH),
    }
