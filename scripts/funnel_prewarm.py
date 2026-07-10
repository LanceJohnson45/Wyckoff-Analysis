# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from datetime import datetime
from pathlib import Path

import pandas as pd
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

from core.stock_cache import get_cache_meta, load_cached_dates, normalize_hist_df, upsert_cache_data
from integrations.data_source import fetch_index_hist, fetch_stock_hist
from integrations.fetch_a_share_csv import _resolve_trading_window, _resolve_us_window, _trade_dates_cached
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
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


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


def _symbol_digest(symbols: list[str]) -> str:
    payload = "\n".join(str(x).strip() for x in symbols if str(x).strip())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


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
        _PREWARM_RUN_STATE_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        _log(f"prewarm state save failed: {type(e).__name__}: {e}")


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
) -> tuple[str, str, int, int, int, int, list[tuple[date, date]], int]:
    end_day = _job_end_calendar_day()
    window = (
        _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
        if market in {"us", "hk"}
        else _resolve_trading_window(end_calendar_day=end_day, trading_days=trading_days)
    )
    if market == "us":
        cache_symbol = f"US:{symbol}"
    elif market == "hk":
        cache_symbol = f"HK:{symbol}"
    else:
        cache_symbol = symbol
    meta = get_cache_meta(cache_symbol, "qfq", context="background")
    expected_dates = _expected_trade_dates(window, market)
    if not expected_dates:
        raise RuntimeError("expected trade dates empty")
    cached_dates = load_cached_dates(
        cache_symbol,
        "qfq",
        window.start_trade_date,
        window.end_trade_date,
        context="background",
    )
    effective_expected_dates = expected_dates
    if meta is not None and _RECENT_GAP_MAX_AGE_DAYS > 0:
        effective_expected_dates = _filter_expected_dates_by_recent_window(
            expected_dates,
            end_trade_date=window.end_trade_date,
            max_age_days=_RECENT_GAP_MAX_AGE_DAYS,
        )
    if (
        meta is not None
        and effective_expected_dates
        and meta.end_date >= effective_expected_dates[-1]
        and meta.end_date >= window.end_trade_date
        and len(cached_dates) >= len(effective_expected_dates)
        and set(effective_expected_dates).issubset(set(cached_dates))
    ):
        return (
            symbol,
            "cache_ready",
            0,
            0,
            len(effective_expected_dates),
            len(cached_dates),
            [],
            0,
        )
    gap_ranges = _missing_ranges(expected_dates, cached_dates)
    if not gap_ranges and meta is None:
        gap_ranges = [(window.start_trade_date, window.end_trade_date)]
    ignored_ranges: list[tuple[date, date]] = []
    if meta is not None and gap_ranges:
        gap_ranges, ignored_ranges = _trim_recent_gap_ranges(
            gap_ranges,
            end_trade_date=window.end_trade_date,
            max_age_days=_RECENT_GAP_MAX_AGE_DAYS,
        )
    if dry_run:
        preview_ranges = gap_ranges[:5]
        if ignored_ranges:
            preview_ranges = preview_ranges + ignored_ranges[:2]
        return (
            symbol,
            "dry_run_missing" if gap_ranges else "dry_run_noop",
            0,
            len(gap_ranges),
            len(effective_expected_dates),
            len(cached_dates),
            gap_ranges[:5],
            len(ignored_ranges),
        )
    rows_written = 0
    for gap_start, gap_end in gap_ranges:
        frame = fetch_stock_hist(
            symbol=symbol,
            start=gap_start,
            end=gap_end,
            adjust="qfq",
            market=market,
        )
        norm = normalize_hist_df(frame)
        if norm is None or norm.empty:
            continue
        ok = upsert_cache_data(
            symbol=cache_symbol,
            adjust="qfq",
            source=str(frame.attrs.get("source", "prewarm_gap_fill") or "prewarm_gap_fill"),
            df=norm,
            context="background",
        )
        if not ok:
            raise RuntimeError(f"gap write failed range={gap_start}..{gap_end}")
        rows_written += int(len(norm.index))
    refreshed_dates = load_cached_dates(
        cache_symbol,
        "qfq",
        window.start_trade_date,
        window.end_trade_date,
        context="background",
    )
    verify_expected_dates = effective_expected_dates
    refreshed_set = set(refreshed_dates)
    if verify_expected_dates and not set(verify_expected_dates).issubset(refreshed_set):
        remaining = _missing_ranges(verify_expected_dates, refreshed_dates)
        return (
            symbol,
            "gap_partial",
            rows_written,
            len(gap_ranges),
            len(verify_expected_dates),
            len(refreshed_dates),
            remaining[:5],
            len(ignored_ranges),
        )
    return (
        symbol,
        "gap_repaired",
        rows_written,
        len(gap_ranges),
        len(verify_expected_dates),
        len(refreshed_dates),
        gap_ranges[:5],
        len(ignored_ranges),
    )


def _parse_manual_symbols(raw: str) -> list[str]:
    return [x.strip() for x in str(raw or "").split(",") if x.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm funnel cache for the current symbol pool")
    parser.add_argument("--market", choices=["cn", "us", "hk"], default=None)
    parser.add_argument("--trading-days", type=int, default=max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30))
    parser.add_argument("--max-workers", type=int, default=max(int(os.getenv("FUNNEL_PREWARM_MAX_WORKERS", "8")), 1))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", help="Only inspect cache gaps; do not fetch or write")
    parser.add_argument("--force", action="store_true", help="Ignore prewarm run-state cache and execute anyway")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols to inspect instead of pool resolution")
    args = parser.parse_args()

    if args.market:
        os.environ["FUNNEL_MARKET"] = args.market
    market = _resolve_funnel_market()
    if args.symbols.strip():
        symbols = _parse_manual_symbols(args.symbols)
        stats = {"pool_mode": "cli_symbols"}
    else:
        symbols, _, stats = _resolve_symbol_pool_from_env()
    normalized = _normalize_symbols(symbols, market=market)
    if args.limit > 0:
        normalized = normalized[: args.limit]
    if not normalized:
        _log(f"prewarm skipped market={market} reason=empty_symbol_pool mode={stats.get('pool_mode')}")
        return 0

    end_day = _job_end_calendar_day()
    if not args.dry_run and not args.force:
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
            return 0

    _log(
        f"prewarm start market={market} symbols={len(normalized)} trading_days={args.trading_days} "
        f"mode={stats.get('pool_mode')} dry_run={args.dry_run} force={args.force}"
    )
    ok = 0
    fail = 0
    cache_ready = 0
    repaired_symbols = 0
    repaired_ranges = 0
    repaired_rows = 0
    with ThreadPoolExecutor(max_workers=max(int(args.max_workers), 1)) as executor:
        futures = {
            executor.submit(
                _prefetch_one,
                sym,
                market,
                max(int(args.trading_days), 1),
                dry_run=bool(args.dry_run),
            ): sym
            for sym in normalized
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                (
                    _,
                    status,
                    rows,
                    gap_count,
                    expected_count,
                    cached_count,
                    gap_preview,
                    ignored_count,
                ) = future.result()
                ok += 1
                if status == "cache_ready":
                    cache_ready += 1
                    _log(
                        f"prewarm ok {symbol} cache_ready "
                        f"cached={cached_count}/{expected_count}"
                    )
                elif status == "gap_partial":
                    repaired_symbols += 1
                    repaired_ranges += int(gap_count)
                    repaired_rows += int(rows)
                    preview = ", ".join(f"{s}..{e}" for s, e in gap_preview) or "-"
                    _log(
                        f"prewarm partial {symbol} rows={rows} "
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
                    _log(f"prewarm ok {symbol} gap_ranges={gap_count} rows={rows}")
            except Exception as e:
                fail += 1
                _log(f"prewarm fail {symbol}: {type(e).__name__}: {e}")
            if args.sleep_seconds > 0:
                time.sleep(max(float(args.sleep_seconds), 0.0))
    _log(
        "prewarm done "
        f"market={market} ok={ok} fail={fail} "
        f"cache_ready={cache_ready} repaired_symbols={repaired_symbols} "
        f"repaired_ranges={repaired_ranges} repaired_rows={repaired_rows}"
    )
    if not args.dry_run and fail == 0:
        state = _load_prewarm_run_state()
        if not isinstance(state, dict):
            state = {}
        state[str(market).lower()] = {
            "status": "ok",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "end_trade_date": end_day.isoformat(),
            "trading_days": int(args.trading_days),
            "symbol_count": len(normalized),
            "symbol_digest": _symbol_digest(normalized),
            "pool_mode": stats.get("pool_mode"),
            "cache_ready": cache_ready,
            "repaired_symbols": repaired_symbols,
            "repaired_ranges": repaired_ranges,
            "repaired_rows": repaired_rows,
        }
        _save_prewarm_run_state(state)
    return 0 if ok > 0 or fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
