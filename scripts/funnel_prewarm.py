# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from contextlib import closing
from datetime import date, timedelta
from datetime import datetime
from functools import lru_cache
from multiprocessing.connection import Connection
from pathlib import Path
from threading import Event
from zoneinfo import ZoneInfo

import pandas as pd
import structlog
from pandas.tseries.holiday import (
    AbstractHolidayCalendar,
    GoodFriday,
    Holiday,
    USLaborDay,
    USMartinLutherKingJr,
    USMemorialDay,
    USPresidentsDay,
    USThanksgivingDay,
    nearest_workday,
)

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

ROOT = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from core.stock_cache import load_cached_dates, normalize_hist_df, upsert_cache_data
from integrations.data_source import fetch_index_hist, fetch_stock_hist
from integrations.market_data_policy import BeforeListingRange, exclusion_applies, fetch_cn_listing_dates, fetch_cn_suspensions, load_security_overrides
from integrations.market_data_review import review_reason, write_review
from integrations.fetch_a_share_csv import _resolve_trading_window, _resolve_us_window, _trade_dates_cached, get_all_stocks, TradingWindow
from scripts.prewarm_workers import PrewarmBudgetExpired, bounded_call, iter_bounded_jobs
from scripts.wyckoff_funnel import (
    HK_MAIN_BENCH_CODE,
    HK_SMALLCAP_BENCH_CODE,
    US_MAIN_BENCH_CODE,
    US_SMALLCAP_BENCH_CODE,
    _job_end_calendar_day,
    _normalize_symbols,
    _resolve_funnel_market,
    _resolve_symbol_pool_from_env,
)


def _log(msg: str) -> None:
    logger = structlog.wrap_logger(structlog.PrintLogger(file=sys.__stdout__ or sys.stdout), processors=[structlog.stdlib.add_log_level, structlog.processors.JSONRenderer(ensure_ascii=False)])
    logger.info(msg, timestamp=datetime.now(ZoneInfo("Asia/Shanghai")).isoformat())


PREWARM_VALIDATION_VERSION = 2

_RECENT_GAP_MAX_AGE_DAYS = max(
    int(os.getenv("FUNNEL_PREWARM_RECENT_GAP_MAX_AGE_DAYS", "45")),
    0,
)
_PREWARM_RUN_STATE_PATH = Path(
    os.getenv(
        "FUNNEL_PREWARM_RUN_STATE_PATH",
        str(ROOT / "data" / "funnel_prewarm_state.json"),
    )
)
_PREWARM_CHECKPOINT_EVERY = max(
    int(os.getenv("FUNNEL_PREWARM_CHECKPOINT_EVERY", "100")),
    1,
)


@lru_cache(maxsize=1)
def _policy_digest() -> str:
    """审核规则变化后，旧检查点必须重新核验。"""
    records = load_security_overrides()
    payload = json.dumps(sorted((market, code, row) for (market, code), row in records.items()), sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def _symbol_digest(symbols: list[str]) -> str:
    payload = "\n".join(str(x).strip() for x in symbols if str(x).strip())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _cache_symbol_for_market(symbol: str, market: str) -> str:
    if market == "us":
        return f"US:{symbol}"
    if market == "hk":
        return f"HK:{symbol}"
    return symbol


def _load_prewarm_run_state() -> dict:
    try:
        if not _PREWARM_RUN_STATE_PATH.exists():
            return {}
        return json.loads(_PREWARM_RUN_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_prewarm_run_state(payload: dict) -> None:
    try:
        _PREWARM_RUN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp_path = _PREWARM_RUN_STATE_PATH.with_suffix(
            f"{_PREWARM_RUN_STATE_PATH.suffix}.tmp"
        )
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(_PREWARM_RUN_STATE_PATH)
    except Exception as e:
        _log(f"prewarm state save failed: {type(e).__name__}: {e}")


def _save_prewarm_progress(
    *,
    run_state: dict,
    market: str,
    status: str,
    end_trade_date: date,
    trading_days: int,
    symbols: list[str],
    pool_mode: object,
    cache_ready: int,
    fast_skipped: int,
    repaired_symbols: int,
    repaired_ranges: int,
    repaired_rows: int,
    ready_updates: dict[str, dict],
) -> int:
    state = run_state if isinstance(run_state, dict) else {}
    previous_market_state = state.get(str(market).lower(), {})
    previous_ready_symbols = (
        previous_market_state.get("ready_symbols", {})
        if isinstance(previous_market_state, dict)
        else {}
    )
    if not isinstance(previous_ready_symbols, dict):
        previous_ready_symbols = {}
    merged_ready_symbols = dict(previous_ready_symbols)
    merged_ready_symbols.update(ready_updates)
    current_cache_symbols = {
        _cache_symbol_for_market(sym, market) for sym in symbols
    }
    merged_ready_symbols = {
        sym: record
        for sym, record in merged_ready_symbols.items()
        if sym in current_cache_symbols
    }
    state[str(market).lower()] = {
        "status": status,
        "validation_version": PREWARM_VALIDATION_VERSION,
        "policy_digest": _policy_digest(),
        "recent_gap_max_age_days": _RECENT_GAP_MAX_AGE_DAYS,
        "updated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "end_trade_date": end_trade_date.isoformat(),
        "trading_days": int(trading_days),
        "symbol_count": len(symbols),
        "symbol_digest": _symbol_digest(symbols),
        "pool_mode": pool_mode,
        "cache_ready": int(cache_ready),
        "fast_skipped": int(fast_skipped),
        "repaired_symbols": int(repaired_symbols),
        "repaired_ranges": int(repaired_ranges),
        "repaired_rows": int(repaired_rows),
        "ready_symbols": merged_ready_symbols,
    }
    _save_prewarm_run_state(state)
    return len(merged_ready_symbols)


def _should_skip_prewarm_run(
    *,
    market: str,
    trading_days: int,
    end_trade_date: date,
    symbols: list[str],
) -> tuple[bool, dict]:
    state = _load_prewarm_run_state()
    market_state = state.get(str(market).lower(), {}) if isinstance(state, dict) else {}
    if not isinstance(market_state, dict):
        return (False, {})
    if market_state.get("validation_version") != PREWARM_VALIDATION_VERSION or market_state.get("policy_digest") != _policy_digest():
        return (False, market_state)
    if market_state.get("recent_gap_max_age_days") != _RECENT_GAP_MAX_AGE_DAYS:
        return (False, market_state)
    if market_state.get("status") != "ok":
        return (False, market_state)
    if str(market_state.get("end_trade_date") or "") != end_trade_date.isoformat():
        return (False, market_state)
    if int(market_state.get("trading_days") or 0) != int(trading_days):
        return (False, market_state)
    if int(market_state.get("symbol_count") or 0) != len(symbols):
        return (False, market_state)
    if str(market_state.get("symbol_digest") or "") != _symbol_digest(symbols):
        return (False, market_state)
    return (True, market_state)


def _symbol_manifest_record_is_ready(
    record: object,
    *,
    end_trade_date: date,
    trading_days: int,
) -> bool:
    if not isinstance(record, dict):
        return False
    if record.get("validation_version") != PREWARM_VALIDATION_VERSION or record.get("policy_digest") != _policy_digest():
        return False
    if record.get("status") != "ready":
        return False
    if str(record.get("end_trade_date") or "") != end_trade_date.isoformat():
        return False
    if int(record.get("trading_days") or 0) != int(trading_days):
        return False
    if int(record.get("recent_gap_max_age_days") or -1) != int(_RECENT_GAP_MAX_AGE_DAYS):
        return False
    return True


def _ready_symbol_manifest_entry(
    *,
    end_trade_date: date,
    trading_days: int,
    expected_count: int,
    cached_count: int,
    listing_date: str = "",
) -> dict:
    return {
        "status": "ready",
        "listing_date": listing_date,
        "validation_version": PREWARM_VALIDATION_VERSION,
        "policy_digest": _policy_digest(),
        "updated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds"),
        "end_trade_date": end_trade_date.isoformat(),
        "trading_days": int(trading_days),
        "recent_gap_max_age_days": int(_RECENT_GAP_MAX_AGE_DAYS),
        "expected_count": int(expected_count),
        "cached_count": int(cached_count),
    }


def _confirmed_listing_date(market: str, symbol: str, overrides: dict, ready_symbols: dict) -> date | None:
    """先用人工证据，再复用成功拉取时记录的明确首次交易日。"""
    policy = overrides.get((market, symbol), {})
    if policy.get("listing_date"):
        return date.fromisoformat(policy["listing_date"])
    record = ready_symbols.get(_cache_symbol_for_market(symbol, market), {}) if isinstance(ready_symbols, dict) else {}
    if isinstance(record, dict) and record.get("validation_version") == PREWARM_VALIDATION_VERSION and record.get("listing_date"):
        return date.fromisoformat(record["listing_date"])
    return None


def _expected_trade_dates(window, market: str) -> list[date]:
    market_norm = str(market or "cn").strip().lower()
    if market_norm == "cn":
        return [
            d for d in _trade_dates_cached()
            if window.start_trade_date <= d <= window.end_trade_date
        ]
    bench_codes: list[tuple[str, str]] = []
    if market_norm == "us":
        bench_codes = [
            ("main_benchmark", US_MAIN_BENCH_CODE),
            ("smallcap_benchmark", US_SMALLCAP_BENCH_CODE),
        ]
    elif market_norm == "hk":
        bench_codes = [
            ("main_benchmark", HK_MAIN_BENCH_CODE),
            ("smallcap_benchmark", HK_SMALLCAP_BENCH_CODE),
        ]
    for _, code in bench_codes:
        try:
            frame = fetch_index_hist(
                code,
                window.start_trade_date,
                window.end_trade_date,
                market=market_norm,
            )
            if frame is None or frame.empty or "date" not in frame.columns:
                continue
            s = pd.to_datetime(frame["date"], errors="coerce").dropna()
            dates = sorted(
                {
                    x.date()
                    for x in s.tolist()
                    if window.start_trade_date <= x.date() <= window.end_trade_date
                }
            )
            if dates:
                return dates
        except Exception:
            continue
    if market_norm == "us":
        return _approx_us_market_dates(
            start=window.start_trade_date,
            end=window.end_trade_date,
        )
    return pd.bdate_range(
        start=window.start_trade_date,
        end=window.end_trade_date,
    ).date.tolist()


class _ApproxNyseHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday("NewYearsDay", month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday(
            "Juneteenth",
            month=6,
            day=19,
            start_date="2022-06-19",
            observance=nearest_workday,
        ),
        Holiday("IndependenceDay", month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday("Christmas", month=12, day=25, observance=nearest_workday),
    ]


def _approx_us_market_dates(*, start: date, end: date) -> list[date]:
    business_days = pd.bdate_range(start=start, end=end)
    if business_days.empty:
        return []
    holidays = _ApproxNyseHolidayCalendar().holidays(
        start=business_days.min(),
        end=business_days.max(),
    )
    holiday_set = {ts.date() for ts in holidays}
    return [ts.date() for ts in business_days if ts.date() not in holiday_set]


def _missing_ranges(expected_dates: list[date], cached_dates: list[date]) -> list[tuple[date, date]]:
    cached_set = set(cached_dates)
    ranges: list[tuple[date, date]] = []
    range_start: date | None = None
    prev_missing: date | None = None
    for day in expected_dates:
        if day in cached_set:
            if range_start is not None and prev_missing is not None:
                ranges.append((range_start, prev_missing))
                range_start = None
                prev_missing = None
            continue
        if range_start is None:
            range_start = day
        prev_missing = day
    if range_start is not None and prev_missing is not None:
        ranges.append((range_start, prev_missing))
    return ranges


def _trim_recent_gap_ranges(
    gap_ranges: list[tuple[date, date]],
    *,
    end_trade_date: date,
    max_age_days: int,
) -> tuple[list[tuple[date, date]], list[tuple[date, date]]]:
    if max_age_days <= 0:
        return (gap_ranges, [])
    cutoff = end_trade_date - timedelta(days=max_age_days)
    kept: list[tuple[date, date]] = []
    ignored: list[tuple[date, date]] = []
    for gap_start, gap_end in gap_ranges:
        if gap_end < cutoff:
            ignored.append((gap_start, gap_end))
            continue
        if gap_start < cutoff <= gap_end:
            kept.append((cutoff, gap_end))
            ignored.append((gap_start, cutoff - timedelta(days=1)))
            continue
        kept.append((gap_start, gap_end))
    return (kept, ignored)


def _filter_expected_dates_by_recent_window(
    expected_dates: list[date],
    *,
    end_trade_date: date,
    max_age_days: int,
) -> list[date]:
    if max_age_days <= 0:
        return expected_dates
    cutoff = end_trade_date - timedelta(days=max_age_days)
    return [d for d in expected_dates if d >= cutoff]


def _prefetch_one(
    symbol: str,
    market: str,
    trading_days: int,
    *,
    dry_run: bool = False,
    end_calendar_day: date | None = None,
    listing_date: date | None = None,
    trading_window: TradingWindow | None = None,
    expected_dates: list[date] | None = None,
    suspension_evidence: str = "",
) -> tuple:
    """优先更新近期行情；上市前不补K，旧缺口失败不能阻止其他缺口。"""
    end_day = end_calendar_day if end_calendar_day is not None else _job_end_calendar_day()
    window = trading_window or (
        _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
        if market in {"us", "hk"}
        else _resolve_trading_window(end_calendar_day=end_day, trading_days=trading_days)
    )
    cache_symbol = _cache_symbol_for_market(symbol, market)
    expected_dates = sorted(set(expected_dates if expected_dates is not None else _expected_trade_dates(window, market)))
    known_listing_date = listing_date
    if not expected_dates:
        raise RuntimeError("expected trade dates empty")
    if listing_date:
        expected_dates = [d for d in expected_dates if d >= listing_date]
    if suspension_evidence:
        expected_dates = [d for d in expected_dates if d != window.end_trade_date]
    cached_dates = sorted(set(load_cached_dates(cache_symbol, "qfq", window.start_trade_date, window.end_trade_date, context="background")))
    effective_expected_dates = expected_dates
    if cached_dates and _RECENT_GAP_MAX_AGE_DAYS > 0:
        effective_expected_dates = _filter_expected_dates_by_recent_window(expected_dates, end_trade_date=window.end_trade_date, max_age_days=_RECENT_GAP_MAX_AGE_DAYS)
    errors: list[str] = []

    def result(status: str, rows: int, gaps: list[tuple[date, date]], ignored: int = 0, *, preview: list[tuple[date, date]] | None = None) -> tuple:
        detail = {"latest_date": str(max(cached_dates)) if cached_dates else "", "target_date": str(window.end_trade_date),
                  "listing_date": str(listing_date) if listing_date else "", "errors": errors.copy(), "suspension_evidence": suspension_evidence}
        return (symbol, status, rows, len(gaps), len(effective_expected_dates), len(cached_dates), (gaps if preview is None else preview)[:5], ignored, detail)

    if not expected_dates:
        return result("suspended" if suspension_evidence else "not_listed", 0, [])
    if cached_dates and set(effective_expected_dates).issubset(cached_dates) and max(cached_dates) >= expected_dates[-1]:
        return result("suspended" if suspension_evidence else "cache_ready", 0, [])
    gap_ranges = _missing_ranges(expected_dates, cached_dates)
    ignored_ranges: list[tuple[date, date]] = []
    # 补数与上面的近期验收使用同一窗口；短缓存也不能反复请求窗口外历史。
    if cached_dates and gap_ranges:
        gap_ranges, ignored_ranges = _trim_recent_gap_ranges(gap_ranges, end_trade_date=window.end_trade_date, max_age_days=_RECENT_GAP_MAX_AGE_DAYS)
    if dry_run:
        return result("dry_run_missing" if gap_ranges else "dry_run_noop", 0, gap_ranges, len(ignored_ranges))
    rows_written = 0
    # 先修复最新缺口，避免上市前或旧停牌区间失败阻断当天行情。
    for gap_start, gap_end in sorted(gap_ranges, key=lambda pair: pair[1], reverse=True):
        if listing_date:
            gap_start = max(gap_start, listing_date)
            if gap_start > gap_end:
                continue
        try:
            # 冷启动且上市日期未知时先发现元数据；明确边界后才强制完整交易日覆盖。
            coverage = {"required_dates": [d for d in effective_expected_dates if gap_start <= d <= gap_end]} if listing_date or cached_dates else {}
            frame = fetch_stock_hist(symbol=symbol, start=gap_start, end=gap_end, adjust="qfq", market=market, **coverage)
            if not known_listing_date and frame.attrs.get("listing_date"):
                listing_date = date.fromisoformat(frame.attrs["listing_date"])
            norm = normalize_hist_df(frame)
            if norm is None or norm.empty:
                raise RuntimeError("行情源返回空日线")
            norm = norm.loc[(norm["date"] >= str(gap_start)) & (norm["date"] <= str(gap_end))].copy()
            if norm.empty:
                raise RuntimeError("行情源返回的日线不在请求区间内")
            if not upsert_cache_data(symbol=cache_symbol, adjust="qfq", source=str(frame.attrs.get("source", "prewarm_gap_fill") or "prewarm_gap_fill"), df=norm, context="background"):
                raise RuntimeError("日线写入失败")
            rows_written += len(norm.index)
        except BeforeListingRange as exc:
            if known_listing_date and known_listing_date != exc.listing_date:
                errors.append(f"{gap_start}..{gap_end}: 已确认上市日期与行情源首次交易日不一致，待审核")
            else:
                listing_date = exc.listing_date
        except Exception as exc:
            errors.append(f"{gap_start}..{gap_end}: {type(exc).__name__}: {exc}")
    if listing_date:
        effective_expected_dates = [d for d in effective_expected_dates if d >= listing_date]
        if listing_date > window.end_trade_date and not errors:
            return result("not_listed", rows_written, [])
    cached_dates = sorted(set(load_cached_dates(cache_symbol, "qfq", window.start_trade_date, window.end_trade_date, context="background")))
    remaining = _missing_ranges(effective_expected_dates, cached_dates)
    if errors or remaining:
        return result("fetch_failed" if errors and not rows_written else "gap_partial", rows_written, gap_ranges, len(ignored_ranges), preview=remaining)
    return result("suspended" if suspension_evidence else "gap_repaired", rows_written, gap_ranges, len(ignored_ranges))


def _parse_manual_symbols(raw: str) -> list[str]:
    return [x.strip() for x in str(raw or "").split(",") if x.strip()]


def _load_cn_universe() -> tuple[list[dict[str, str]], list[str]]:
    """在隔离进程内取得名单及降级警告，避免回调状态丢失。"""
    warnings: list[str] = []
    items = get_all_stocks(force_refresh=True, on_warning=warnings.append)
    return items, warnings


def _configure_prewarm_worker(connection: Connection) -> None:
    """限频槽由父进程统一分配，子进程被强杀也不会遗留锁。"""
    from integrations import data_source

    def wait_tickflow_slot() -> None:
        interval = data_source._TICKFLOW_DAILY_MIN_INTERVAL_SECONDS
        if interval > 0:
            connection.send(("rate_limit", interval))
            reserved = connection.recv()
            Event().wait(max(reserved - time.monotonic(), 0))

    data_source._wait_tickflow_daily_slot = wait_tickflow_slot


def main() -> int:
    """限时维护日线并保存成功、失败及未派发的审核记录。"""
    outer_seconds = float(os.getenv("FUNNEL_PREWARM_TIMEOUT_MINUTES", "70")) * 60
    default_run_budget = max(outer_seconds - 300, 1)
    parser = argparse.ArgumentParser(description="Warm funnel cache for the current symbol pool")
    parser.add_argument("--market", choices=["cn", "us", "hk"], default=None)
    parser.add_argument("--trading-days", type=int, default=max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30))
    parser.add_argument("--max-workers", type=int, default=max(int(os.getenv("FUNNEL_PREWARM_MAX_WORKERS", "8")), 1))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", help="Only inspect cache gaps; do not fetch or write")
    parser.add_argument("--force", action="store_true", help="Ignore prewarm run-state cache and execute anyway")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to inspect instead of pool resolution")
    parser.add_argument("--universe", choices=["strategy", "all"], default="strategy", help="all维护全A股（包含ST/科创/北交所），不沿用策略筛选池")
    parser.add_argument("--review-dir", type=Path, default=ROOT / "logs")
    parser.add_argument("--end-date", type=date.fromisoformat, default=None, help="固定本轮截止日，YYYY-MM-DD")
    parser.add_argument("--symbol-timeout-seconds", type=float, default=60, help="单票总时限，超时终止对应子进程")
    parser.add_argument("--dispatch-budget-seconds", type=float, default=max(default_run_budget - 300, 1), help="启动后停止派发新股票的秒数")
    parser.add_argument("--run-budget-seconds", type=float, default=default_run_budget, help="启动后结束在途任务的秒数；必须小于Action外层时限")
    args = parser.parse_args()
    if not (0 < args.symbol_timeout_seconds < float("inf") and 0 < args.dispatch_budget_seconds <= args.run_budget_seconds < float("inf")):
        parser.error("单票时限须为正数，且0 < 派发预算 <= 收尾预算")
    if "FUNNEL_PREWARM_TIMEOUT_MINUTES" in os.environ and args.run_budget_seconds >= outer_seconds:
        parser.error("收尾预算必须小于Action外层预热时限")
    started = time.monotonic()
    dispatch_deadline = started + args.dispatch_budget_seconds
    run_deadline = started + args.run_budget_seconds

    if args.market:
        os.environ["FUNNEL_MARKET"] = args.market
    market = _resolve_funnel_market()
    end_day = args.end_date or _job_end_calendar_day()
    if args.symbols.strip():
        symbols = _parse_manual_symbols(args.symbols)
        stats = {"pool_mode": "cli_symbols"}
    elif market == "cn" and args.universe == "all":
        items, universe_warnings = bounded_call(_load_cn_universe, timeout=min(180, max(run_deadline - time.monotonic(), 0.01)))
        symbols = [item["code"] for item in items]
        stats = {"pool_mode": "all_cn", "names": {item["code"]: item.get("name", "") for item in items}, "universe_warning": "; ".join(universe_warnings)}
    else:
        symbols, names, stats = bounded_call(_resolve_symbol_pool_from_env, timeout=min(180, max(run_deadline - time.monotonic(), 0.01)))
        stats["names"] = names
    normalized = _normalize_symbols(symbols, market=market)
    if args.limit > 0:
        normalized = normalized[: args.limit]
    if not normalized:
        _log(f"prewarm skipped market={market} reason=empty_symbol_pool mode={stats.get('pool_mode')}")
        return 1

    run_window = bounded_call(_resolve_us_window if market in {"us", "hk"} else _resolve_trading_window, end_calendar_day=end_day, trading_days=max(int(args.trading_days), 1), timeout=min(180, max(run_deadline - time.monotonic(), 0.01)))
    run_dates = bounded_call(_expected_trade_dates, run_window, market, timeout=min(180, max(run_deadline - time.monotonic(), 0.01)))
    end_day = run_window.end_trade_date
    overrides = load_security_overrides()
    records = {sym: {"market": market, "symbol": sym, "name": stats.get("names", {}).get(sym, ""), "status": "pending", "target_date": str(end_day)} for sym in normalized}
    excluded = []
    for sym in normalized:
        policy = overrides.get((market, sym), {})
        if exclusion_applies(policy, end_day):
            excluded.append(sym)
            records[sym].update(status="excluded", reason=policy["reason"], evidence=policy["evidence"], suggested_action="已人工确认排除；保留历史数据，不再拉取")
    normalized = [sym for sym in normalized if sym not in set(excluded)]
    write_review(args.review_dir, market, str(end_day), records, pool_mode=str(stats.get("pool_mode")), status="running", universe_warning=str(stats.get("universe_warning", "")))
    if not args.dry_run and not args.force and not stats.get("universe_warning"):
        should_skip, cached_state = _should_skip_prewarm_run(
            market=market,
            trading_days=max(int(args.trading_days), 1),
            end_trade_date=end_day,
            symbols=normalized,
        )
        if should_skip:
            _log(
                "prewarm skipped "
                f"market={market} reason=run_state_cache "
                f"trade_date={end_day.isoformat()} "
                f"symbols={len(normalized)} trading_days={int(args.trading_days)} "
                f"updated_at={cached_state.get('updated_at', '')}"
            )
            for sym in normalized:
                records[sym].update(status="manifest_cached", latest_date=str(end_day))
            write_review(args.review_dir, market, str(end_day), records, pool_mode=str(stats.get("pool_mode")), status="ok", final=True, universe_warning=str(stats.get("universe_warning", "")))
            return 0

    run_trading_days = max(int(args.trading_days), 1)
    _log(
        f"prewarm start market={market} symbols={len(normalized)} trading_days={args.trading_days} "
        f"mode={stats.get('pool_mode')} dry_run={args.dry_run} force={args.force}"
    )
    ok = 0
    fail = 0
    partial = 0
    deferred = 0
    cache_ready = 0
    repaired_symbols = 0
    repaired_ranges = 0
    repaired_rows = 0
    fast_skipped = 0
    symbols_to_check = normalized
    run_state = _load_prewarm_run_state()
    market_state = (
        run_state.get(str(market).lower(), {}) if isinstance(run_state, dict) else {}
    )
    ready_symbols = (
        market_state.get("ready_symbols", {}) if isinstance(market_state, dict) else {}
    )
    if (
        not args.dry_run
        and not args.force
        and isinstance(ready_symbols, dict)
        and ready_symbols
    ):
        pending: list[str] = []
        for sym in normalized:
            cache_symbol = _cache_symbol_for_market(sym, market)
            if _symbol_manifest_record_is_ready(
                ready_symbols.get(cache_symbol),
                end_trade_date=end_day,
                trading_days=run_trading_days,
            ):
                fast_skipped += 1
                records[sym].update(status="manifest_cached", latest_date=str(end_day))
                continue
            pending.append(sym)
        symbols_to_check = pending
        if fast_skipped:
            ok += fast_skipped
            cache_ready += fast_skipped
            _log(
                "prewarm manifest hit "
                f"market={market} fast_skipped={fast_skipped} "
                f"pending={len(symbols_to_check)} trade_date={end_day.isoformat()}"
            )

    ready_updates: dict[str, dict] = {}
    suspensions: dict[str, str] = {}
    if market == "cn" and symbols_to_check:
        for exchange, prefixes in (("bj", ("4", "8", "92")), ("sz", ("0", "3"))):
            candidates = [sym for sym in symbols_to_check if sym.startswith(prefixes) and not overrides.get((market, sym), {}).get("listing_date")]
            if not candidates:
                continue
            try:
                metadata = bounded_call(fetch_cn_listing_dates, exchange, timeout=min(30, max(run_deadline - time.monotonic(), 0.01)))
                applied = 0
                for sym in candidates:
                    if sym in metadata:
                        row = metadata[sym]
                        overrides.setdefault((market, sym), {}).update(listing_date=row["listing_date"])
                        records[sym]["evidence"] = row["evidence"]
                        applied += 1
                _log(f"prewarm listing metadata exchange={exchange} applied={applied}")
            except Exception as exc:
                _log(f"prewarm listing lookup failed exchange={exchange}: {type(exc).__name__}: {exc}; 未确认边界仍保留缺口审核")
        try:
            suspensions = bounded_call(fetch_cn_suspensions, end_day, timeout=min(30, max(run_deadline - time.monotonic(), 0.01)))
        except Exception as exc:
            _log(f"prewarm suspension lookup failed: {type(exc).__name__}: {exc}; 未确认停牌仍按正常缺口核验")
    completed_since_checkpoint = 0
    jobs = ((sym, (sym, market, run_trading_days), {
        "dry_run": bool(args.dry_run), "end_calendar_day": end_day,
        "trading_window": run_window, "expected_dates": run_dates,
        "listing_date": _confirmed_listing_date(market, sym, overrides, ready_symbols),
        "suspension_evidence": suspensions.get(sym, ""),
    }) for sym in symbols_to_check)
    with closing(iter_bounded_jobs(_prefetch_one, jobs, max_workers=max(int(args.max_workers), 1),
                                   task_timeout=args.symbol_timeout_seconds,
                                   dispatch_deadline=dispatch_deadline, run_deadline=run_deadline,
                                   initializer=_configure_prewarm_worker,
                                   dispatch_interval=max(args.sleep_seconds, 0))) as outcomes:
        for symbol, future in outcomes:
            if isinstance(ready_symbols, dict):
                ready_symbols.pop(_cache_symbol_for_market(symbol, market), None)
            try:
                outcome = future.result()
                (
                    _,
                    status,
                    rows,
                    gap_count,
                    expected_count,
                    cached_count,
                    gap_preview,
                    ignored_count,
                ) = outcome[:8]
                detail = outcome[8] if len(outcome) > 8 else {}
                if status not in {"cache_ready", "gap_repaired", "suspended", "not_listed", "dry_run_missing", "dry_run_noop", "gap_partial", "fetch_failed"}:
                    raise ValueError(f"未知预热结果状态：{status}")
                records[symbol].update(status=status, target_date=detail.get("target_date", str(end_day)), latest_date=detail.get("latest_date", ""), listing_date=detail.get("listing_date", ""), missing_ranges=", ".join(f"{a}..{b}" for a, b in gap_preview) if status in {"gap_partial", "fetch_failed", "dry_run_missing"} else "")
                if detail.get("errors"):
                    records[symbol]["detail"] = " | ".join(detail["errors"])[:2000]
                    reason, action = review_reason(records[symbol]["detail"])
                    records[symbol].update(reason=reason, suggested_action=action)
                if status == "suspended":
                    records[symbol].update(reason="confirmed_full_day_suspension", evidence=detail["suspension_evidence"], suggested_action="仅目标日免补日线；下次运行重新核验停复牌状态")
                if status in {"gap_partial", "fetch_failed"}:
                    partial += int(status == "gap_partial")
                    fail += int(status == "fetch_failed")
                    records[symbol]["status"] = "failed" if status == "fetch_failed" else status
                    records[symbol].setdefault("reason", "missing_trading_dates")
                    records[symbol].setdefault("suggested_action", "核对停牌、上市日与行情源；补齐后重跑")
                else:
                    ok += 1
                if status == "cache_ready":
                    cache_ready += 1
                    ready_updates[_cache_symbol_for_market(symbol, market)] = (
                        _ready_symbol_manifest_entry(
                            end_trade_date=end_day,
                            trading_days=run_trading_days,
                            expected_count=expected_count,
                            cached_count=cached_count,
                            listing_date=detail.get("listing_date", ""),
                        )
                    )
                    _log(
                        f"prewarm ok {symbol} cache_ready "
                        f"cached={cached_count}/{expected_count}"
                    )
                elif status == "suspended":
                    _log(f"prewarm suspended {symbol} target_date={end_day} rows={rows}")
                elif status in {"gap_partial", "fetch_failed"}:
                    repaired_symbols += 1
                    repaired_ranges += int(gap_count)
                    repaired_rows += int(rows)
                    preview = ", ".join(f"{s}..{e}" for s, e in gap_preview) or "-"
                    _log(
                        f"prewarm {'fail' if status == 'fetch_failed' else 'partial'} {symbol} rows={rows} "
                        f"remaining_ranges=[{preview}] ignored_old_ranges={ignored_count}"
                    )
                elif args.dry_run:
                    repaired_ranges += int(gap_count)
                    preview = ", ".join(f"{s}..{e}" for s, e in gap_preview) or "-"
                    _log(
                        f"prewarm dry-run {symbol} status={status} "
                        f"cached={cached_count}/{expected_count} "
                        f"gap_ranges={gap_count} ignored_old_ranges={ignored_count} "
                        f"preview=[{preview}]"
                    )
                else:
                    repaired_symbols += 1
                    repaired_ranges += int(gap_count)
                    repaired_rows += int(rows)
                    if status == "gap_repaired":
                        ready_updates[_cache_symbol_for_market(symbol, market)] = (
                            _ready_symbol_manifest_entry(
                                end_trade_date=end_day,
                                trading_days=run_trading_days,
                                expected_count=expected_count,
                                cached_count=cached_count,
                                listing_date=detail.get("listing_date", ""),
                            )
                        )
                    _log(f"prewarm ok {symbol} gap_ranges={gap_count} rows={rows}")
            except PrewarmBudgetExpired as e:
                deferred += 1
                records[symbol].update(status="pending", reason="batch_deadline", suggested_action="批次预算耗尽；保留已写入行情，下轮续跑", detail=str(e))
                _log(f"prewarm deferred {symbol}: {e}")
            except Exception as e:
                fail += 1
                reason, action = review_reason(str(e))
                if isinstance(e, TimeoutError):
                    reason, action = "symbol_timeout", "单票总时限已到，执行进程已回收；检查上游耗时后续跑"
                records[symbol].update(status="failed", reason=reason, suggested_action=action, detail=f"{type(e).__name__}: {e}")
                _log(f"prewarm fail {symbol}: {type(e).__name__}: {e}")
            completed_since_checkpoint += 1
            if (
                not args.dry_run
                and completed_since_checkpoint >= _PREWARM_CHECKPOINT_EVERY
            ):
                manifest_ready = _save_prewarm_progress(
                    run_state=run_state,
                    market=market,
                    status="partial",
                    end_trade_date=end_day,
                    trading_days=run_trading_days,
                    symbols=normalized,
                    pool_mode=stats.get("pool_mode"),
                    cache_ready=cache_ready,
                    fast_skipped=fast_skipped,
                    repaired_symbols=repaired_symbols,
                    repaired_ranges=repaired_ranges,
                    repaired_rows=repaired_rows,
                    ready_updates=ready_updates,
                )
                # 保存会替换市场状态字典，后续失败必须清除新检查点里的ready。
                ready_symbols = run_state[str(market).lower()]["ready_symbols"]
                _log(
                    "prewarm checkpoint "
                    f"market={market} completed={ok + fail + partial + deferred} "
                    f"manifest_ready={manifest_ready} fail={fail}"
                )
                write_review(args.review_dir, market, str(end_day), records, pool_mode=str(stats.get("pool_mode")), status="partial", universe_warning=str(stats.get("universe_warning", "")))
                completed_since_checkpoint = 0
    _log(
        "prewarm done "
        f"market={market} ok={ok} fail={fail} partial={partial} deferred={deferred} excluded={len(excluded)} "
        f"cache_ready={cache_ready} fast_skipped={fast_skipped} "
        f"repaired_symbols={repaired_symbols} "
        f"repaired_ranges={repaired_ranges} repaired_rows={repaired_rows}"
    )
    final_status = "partial" if deferred else "dry_run" if args.dry_run else "partial" if fail or partial or stats.get("universe_warning") else "ok"
    if not args.dry_run:
        checkpoint_status = final_status
        # 含停牌的轮次必须再次查询证据，不能整轮跳过后漏掉复牌。
        if final_status == "ok" and any(row["status"] == "suspended" for row in records.values()):
            checkpoint_status = "suspended"
        _save_prewarm_progress(
            run_state=run_state,
            market=market,
            status=checkpoint_status,
            end_trade_date=end_day,
            trading_days=run_trading_days,
            symbols=normalized,
            pool_mode=stats.get("pool_mode"),
            cache_ready=cache_ready,
            fast_skipped=fast_skipped,
            repaired_symbols=repaired_symbols,
            repaired_ranges=repaired_ranges,
            repaired_rows=repaired_rows,
            ready_updates=ready_updates,
        )
    write_review(args.review_dir, market, str(end_day), records, pool_mode=str(stats.get("pool_mode")), status=final_status, final=True, universe_warning=str(stats.get("universe_warning", "")))
    return 0 if not deferred and (args.dry_run or (fail == 0 and partial == 0 and not stats.get("universe_warning"))) else 2


if __name__ == "__main__":
    raise SystemExit(main())
