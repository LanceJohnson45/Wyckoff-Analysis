# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_cache import load_cached_dates, normalize_hist_df, upsert_cache_data
from integrations.data_source import _cn_stock_to_yfinance_symbol, _fetch_stock_yfinance
from integrations.fetch_a_share_csv import _trade_dates_cached, get_all_stocks
from utils.trading_clock import resolve_end_calendar_day_for_market


DEFAULT_LOOKBACK_DAYS = max(int(os.getenv("CN_A_BACKFILL_LOOKBACK_DAYS", "365")), 30)
DEFAULT_SLEEP_SECONDS = max(float(os.getenv("CN_A_BACKFILL_SLEEP_SECONDS", "1.6")), 0.0)
DEFAULT_JITTER_SECONDS = max(float(os.getenv("CN_A_BACKFILL_JITTER_SECONDS", "0.6")), 0.0)
DEFAULT_RETRY_TIMES = max(int(os.getenv("CN_A_BACKFILL_RETRY_TIMES", "4")), 1)
DEFAULT_RETRY_BACKOFF_SECONDS = max(
    float(os.getenv("CN_A_BACKFILL_RETRY_BACKOFF_SECONDS", "8.0")),
    0.0,
)
DEFAULT_GAP_CHUNK_DAYS = max(int(os.getenv("CN_A_BACKFILL_GAP_CHUNK_DAYS", "120")), 10)
DEFAULT_PROGRESS_EVERY = max(int(os.getenv("CN_A_BACKFILL_PROGRESS_EVERY", "25")), 1)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _snap_window_to_trade_dates(
    start_day: date,
    end_day: date,
) -> tuple[date, date, list[date]]:
    trade_dates = [d for d in _trade_dates_cached() if start_day <= d <= end_day]
    if not trade_dates:
        raise RuntimeError(
            f"no CN trade dates found in requested window {start_day}..{end_day}"
        )
    return trade_dates[0], trade_dates[-1], trade_dates


def _resolve_target_window(
    *,
    start_date: str | None,
    end_date: str | None,
    lookback_days: int,
) -> tuple[date, date]:
    if end_date:
        end_day = pd.to_datetime(end_date, errors="coerce").date()
    else:
        end_day = resolve_end_calendar_day_for_market("cn")
    if start_date:
        start_day = pd.to_datetime(start_date, errors="coerce").date()
    else:
        start_day = end_day - timedelta(days=lookback_days)
    if start_day > end_day:
        raise ValueError("start_date must be <= end_date")
    snapped_start, snapped_end, trade_dates = _snap_window_to_trade_dates(
        start_day,
        end_day,
    )
    if snapped_start != start_day or snapped_end != end_day:
        _log(
            f"window snapped from {start_day}..{end_day} "
            f"to trading window {snapped_start}..{snapped_end}"
        )
    return snapped_start, snapped_end


def _missing_ranges(
    expected_dates: list[date],
    cached_dates: list[date],
) -> list[tuple[date, date]]:
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


def _split_gap_ranges(
    ranges: list[tuple[date, date]],
    *,
    max_span_days: int,
) -> list[tuple[date, date]]:
    if max_span_days <= 0:
        return ranges
    out: list[tuple[date, date]] = []
    for start_day, end_day in ranges:
        cursor = start_day
        while cursor <= end_day:
            chunk_end = min(cursor + timedelta(days=max_span_days - 1), end_day)
            out.append((cursor, chunk_end))
            cursor = chunk_end + timedelta(days=1)
    return out


def _sleep_between_symbols(base_seconds: float, jitter_seconds: float) -> None:
    sleep_for = max(base_seconds, 0.0)
    if jitter_seconds > 0:
        sleep_for += random.uniform(0.0, jitter_seconds)
    if sleep_for > 0:
        time.sleep(sleep_for)


def _load_supported_symbols(limit: int = 0) -> tuple[list[str], int]:
    supported: list[str] = []
    unsupported = 0
    for row in get_all_stocks():
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        if _cn_stock_to_yfinance_symbol(code):
            supported.append(code)
        else:
            unsupported += 1
    supported = sorted(set(supported))
    if limit > 0:
        supported = supported[:limit]
    return supported, unsupported


def _fetch_gap_frame(
    *,
    symbol: str,
    yf_symbol: str,
    gap_start: date,
    gap_end: date,
    retry_times: int,
    retry_backoff_seconds: float,
) -> pd.DataFrame:
    last_err: Exception | None = None
    for attempt in range(1, retry_times + 1):
        try:
            return _fetch_stock_yfinance(
                yf_symbol,
                gap_start.strftime("%Y%m%d"),
                gap_end.strftime("%Y%m%d"),
            )
        except Exception as e:
            last_err = e
            _log(
                f"fetch retry symbol={symbol} yf={yf_symbol} range={gap_start}..{gap_end} "
                f"attempt={attempt}/{retry_times} err={type(e).__name__}: {e}"
            )
            if attempt < retry_times:
                time.sleep(retry_backoff_seconds * attempt)
    raise RuntimeError(
        f"fetch failed for {symbol} {gap_start}..{gap_end}: {last_err}"
    ) from last_err


def _upsert_symbol_range(symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    norm = normalize_hist_df(df)
    if norm.empty:
        return 0
    ok = upsert_cache_data(
        symbol=symbol,
        adjust="qfq",
        source="yfinance_backfill",
        df=norm,
        context="background",
    )
    return int(len(norm)) if ok else 0


def backfill_symbol(
    *,
    symbol: str,
    start_day: date,
    end_day: date,
    expected_dates: list[date],
    gap_chunk_days: int,
    retry_times: int,
    retry_backoff_seconds: float,
) -> dict[str, object]:
    yf_symbol = _cn_stock_to_yfinance_symbol(symbol)
    if not yf_symbol:
        return {"symbol": symbol, "status": "unsupported", "rows": 0, "ranges": []}

    cached_dates = load_cached_dates(
        symbol,
        "qfq",
        start_day,
        end_day,
        context="background",
    )
    gap_ranges = _missing_ranges(expected_dates, cached_dates)
    if not gap_ranges:
        return {
            "symbol": symbol,
            "status": "cache_ready",
            "rows": 0,
            "ranges": [],
            "missing_dates": 0,
        }

    split_ranges = _split_gap_ranges(gap_ranges, max_span_days=gap_chunk_days)
    written_rows = 0
    for gap_start, gap_end in split_ranges:
        frame = _fetch_gap_frame(
            symbol=symbol,
            yf_symbol=yf_symbol,
            gap_start=gap_start,
            gap_end=gap_end,
            retry_times=retry_times,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        rows = _upsert_symbol_range(symbol, frame)
        if rows <= 0:
            raise RuntimeError(
                f"upsert failed for {symbol} {gap_start}..{gap_end}"
            )
        written_rows += rows

    refreshed_dates = load_cached_dates(
        symbol,
        "qfq",
        start_day,
        end_day,
        context="background",
    )
    still_missing = sorted(set(expected_dates) - set(refreshed_dates))
    if still_missing:
        raise RuntimeError(
            f"write verify failed for {symbol}, missing {len(still_missing)} dates, "
            f"first_missing={still_missing[:5]}"
        )

    return {
        "symbol": symbol,
        "status": "updated",
        "rows": written_rows,
        "ranges": [(a.isoformat(), b.isoformat()) for a, b in split_ranges],
        "missing_dates": len(set(expected_dates) - set(cached_dates)),
    }


def run_backfill(args: argparse.Namespace) -> int:
    start_day, end_day = _resolve_target_window(
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=max(int(args.lookback_days), 1),
    )
    start_day, end_day, expected_dates = _snap_window_to_trade_dates(start_day, end_day)
    if not expected_dates:
        raise RuntimeError(f"no CN trade dates found in {start_day}..{end_day}")

    symbols, unsupported_universe = _load_supported_symbols(limit=max(int(args.limit), 0))
    if not symbols:
        raise RuntimeError("no A-share symbols available for yfinance backfill")

    stats = {
        "symbols_total": len(symbols),
        "symbols_cache_ready": 0,
        "symbols_updated": 0,
        "symbols_failed": 0,
        "symbols_unsupported": unsupported_universe,
        "rows_written": 0,
        "expected_trade_dates": len(expected_dates),
        "failed_symbols": [],
    }

    _log(
        "cn a-share yfinance backfill start "
        f"symbols={len(symbols)} unsupported={unsupported_universe} "
        f"window={start_day}..{end_day} trade_dates={len(expected_dates)} "
        f"sleep={args.sleep_seconds}s jitter={args.jitter_seconds}s "
        f"gap_chunk_days={args.gap_chunk_days}"
    )

    for idx, symbol in enumerate(symbols, start=1):
        try:
            result = backfill_symbol(
                symbol=symbol,
                start_day=start_day,
                end_day=end_day,
                expected_dates=expected_dates,
                gap_chunk_days=max(int(args.gap_chunk_days), 1),
                retry_times=max(int(args.retry_times), 1),
                retry_backoff_seconds=max(float(args.retry_backoff_seconds), 0.0),
            )
            status = str(result.get("status"))
            if status == "cache_ready":
                stats["symbols_cache_ready"] += 1
            elif status == "updated":
                stats["symbols_updated"] += 1
                stats["rows_written"] += int(result.get("rows", 0) or 0)
                _log(
                    f"updated {symbol} rows={result.get('rows', 0)} "
                    f"ranges={result.get('ranges', [])}"
                )
            else:
                stats["symbols_unsupported"] += 1
        except Exception as e:
            stats["symbols_failed"] += 1
            stats["failed_symbols"].append({"symbol": symbol, "error": str(e)})
            _log(f"failed {symbol}: {type(e).__name__}: {e}")
        if idx % max(int(args.progress_every), 1) == 0 or idx == len(symbols):
            _log(
                f"progress {idx}/{len(symbols)} cache_ready={stats['symbols_cache_ready']} "
                f"updated={stats['symbols_updated']} failed={stats['symbols_failed']} "
                f"rows={stats['rows_written']}"
            )
        if idx < len(symbols):
            _sleep_between_symbols(
                base_seconds=float(args.sleep_seconds),
                jitter_seconds=float(args.jitter_seconds),
            )

    _log(f"cn a-share yfinance backfill done {json.dumps(stats, ensure_ascii=False)}")
    return 0 if (stats["symbols_updated"] > 0 or stats["symbols_cache_ready"] > 0) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill one year of A-share daily bars into stock_hist_cache via yfinance"
    )
    parser.add_argument("--start-date", default=os.getenv("CN_A_BACKFILL_START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("CN_A_BACKFILL_END_DATE", ""))
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--jitter-seconds", type=float, default=DEFAULT_JITTER_SECONDS)
    parser.add_argument("--retry-times", type=int, default=DEFAULT_RETRY_TIMES)
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=DEFAULT_RETRY_BACKOFF_SECONDS,
    )
    parser.add_argument("--gap-chunk-days", type=int, default=DEFAULT_GAP_CHUNK_DAYS)
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    parser.add_argument(
        "--limit",
        type=int,
        default=max(int(os.getenv("CN_A_BACKFILL_LIMIT", "0")), 0),
        help="Limit symbol count for smoke/debug runs; 0 means all supported symbols",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_backfill(args)


if __name__ == "__main__":
    raise SystemExit(main())
