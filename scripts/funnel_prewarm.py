# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from datetime import datetime

import pandas as pd

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_cache import get_cache_meta, load_cached_dates, normalize_hist_df, upsert_cache_data
from integrations.data_source import fetch_stock_hist
from integrations.fetch_a_share_csv import _resolve_trading_window, _resolve_us_window, _trade_dates_cached
from scripts.wyckoff_funnel import _job_end_calendar_day, _normalize_symbols, _resolve_funnel_market, _resolve_symbol_pool_from_env


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _expected_trade_dates(window, market: str) -> list[date]:
    market_norm = str(market or "cn").strip().lower()
    if market_norm == "cn":
        return [
            d for d in _trade_dates_cached()
            if window.start_trade_date <= d <= window.end_trade_date
        ]
    return pd.bdate_range(
        start=window.start_trade_date,
        end=window.end_trade_date,
    ).date.tolist()


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


def _prefetch_one(symbol: str, market: str, trading_days: int) -> tuple[str, str, int, int]:
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
    if (
        meta is not None
        and meta.start_date <= window.start_trade_date
        and meta.end_date >= window.end_trade_date
        and len(cached_dates) == len(expected_dates)
        and cached_dates == expected_dates
    ):
        return symbol, "cache_ready", 0, 0
    gap_ranges = _missing_ranges(expected_dates, cached_dates)
    if not gap_ranges and meta is None:
        gap_ranges = [(window.start_trade_date, window.end_trade_date)]
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
    if len(refreshed_dates) != len(expected_dates) or refreshed_dates != expected_dates:
        remaining = _missing_ranges(expected_dates, refreshed_dates)
        raise RuntimeError(
            f"cache gap verification failed remaining_ranges={remaining[:3]}"
        )
    return symbol, "gap_repaired", rows_written, len(gap_ranges)


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm funnel cache for the current symbol pool")
    parser.add_argument("--market", choices=["cn", "us", "hk"], default=None)
    parser.add_argument("--trading-days", type=int, default=max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30))
    parser.add_argument("--max-workers", type=int, default=max(int(os.getenv("FUNNEL_PREWARM_MAX_WORKERS", "8")), 1))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    if args.market:
        os.environ["FUNNEL_MARKET"] = args.market
    market = _resolve_funnel_market()
    symbols, _, stats = _resolve_symbol_pool_from_env()
    normalized = _normalize_symbols(symbols, market=market)
    if args.limit > 0:
        normalized = normalized[: args.limit]
    if not normalized:
        _log(f"prewarm skipped market={market} reason=empty_symbol_pool mode={stats.get('pool_mode')}")
        return 0

    _log(
        f"prewarm start market={market} symbols={len(normalized)} trading_days={args.trading_days} mode={stats.get('pool_mode')}"
    )
    ok = 0
    fail = 0
    cache_ready = 0
    repaired_symbols = 0
    repaired_ranges = 0
    repaired_rows = 0
    with ThreadPoolExecutor(max_workers=max(int(args.max_workers), 1)) as executor:
        futures = {executor.submit(_prefetch_one, sym, market, max(int(args.trading_days), 1)): sym for sym in normalized}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                _, status, rows, gap_count = future.result()
                ok += 1
                if status == "cache_ready":
                    cache_ready += 1
                    _log(f"prewarm ok {symbol} cache_ready")
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
    return 0 if ok > 0 or fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
