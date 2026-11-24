"""Operator-only refresh of an explicit research pool plus SPY/QQQ/IWM."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, TypedDict

import psycopg

if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from integrations.market_data_lock import us_market_job
from integrations.postgres_base import fetch_all
from scripts.refresh_market_benchmarks import refresh_benchmark
from scripts.us_market_target import verify_target
from scripts.us_sp500_maintenance import _run_batches
from scripts.us_yfinance_backfill import _approx_us_market_dates
from utils.trading_clock import resolve_end_calendar_day_for_market

RESEARCH_BENCHMARKS = ("SPY", "QQQ", "IWM")


class RefreshResult(TypedDict):
    as_of: str
    symbols: int
    symbols_hash: str
    stats: dict[str, int]
    benchmarks: dict[str, int]
    missing_target: list[str]
    status: Literal["completed", "incomplete"]
    quality_checked: Literal[False]


def covered_symbols(symbols: list[str], as_of: date, history_rows: int) -> set[str]:
    """Reuse already committed windows; the research snapshot still checks exact sessions."""
    rows = fetch_all(
        "select symbol from public.stock_hist_cache where symbol = any(%s) and adjust = 'qfq' and date <= %s "
        "and open > 0 and high >= open and high >= close and low > 0 and low <= open and low <= close "
        "and volume >= 0 and close > 0 and high >= low group by symbol "
        "having count(*) >= %s and max(date) = %s",
        ([f"US:{symbol}" for symbol in symbols], as_of, history_rows, as_of),
    )
    return {row["symbol"].removeprefix("US:") for row in rows}


def research_symbols(raw: str) -> list[str]:
    """Require an explicit frozen pool; do not fall back to a live S&P-only list."""
    values = [
        value.strip().upper().removeprefix("US:").replace(".", "-")
        for value in raw.split(",")
    ]
    if not 1 <= len(values) <= 2000 or any(
        not re.fullmatch(r"[A-Z][A-Z0-9]{0,9}(?:-[A-Z0-9]{1,4})?", value)
        for value in values
    ):
        raise ValueError("股票池须为1–2000个有效US代码，逗号分隔，不接受空项")
    return sorted(set(values))


def refresh_research(
    symbols: list[str],
    as_of: date,
    *,
    trading_days: int = 360,
    batch_size: int = 40,
    sleep_seconds: float = 0.5,
) -> RefreshResult:
    """Reuse production adjusted-history downloads/writes under one fixed-date job."""
    symbols = research_symbols(",".join(symbols))
    if (
        not 360 <= trading_days <= 1000
        or not 1 <= batch_size <= 100
        or not 0 <= sleep_seconds <= 60
    ):
        raise ValueError("维护窗口360–1000、批量1–100、批间等待0–60秒")
    latest = resolve_end_calendar_day_for_market("us")
    if as_of > latest or as_of not in _approx_us_market_dates(
        start=as_of - timedelta(days=10), end=as_of
    ):
        raise ValueError("as_of必须为已收盘US交易日；不自动滚动到其他日期")
    days = _approx_us_market_dates(
        start=as_of - timedelta(days=trading_days * 2), end=as_of
    )
    start = days[-trading_days] if len(days) >= trading_days else days[0]
    with us_market_job():
        covered = covered_symbols(symbols, as_of, trading_days)
        verify_target(as_of)
        benchmarks = {
            symbol: refresh_benchmark(
                "us", calendar_days=trading_days * 2, end_date=as_of, benchmark=symbol
            )
            for symbol in RESEARCH_BENCHMARKS
        }
        stats = _run_batches(
            [symbol for symbol in symbols if symbol not in covered],
            start_day=start,
            end_day=as_of,
            batch_size=batch_size,
            sleep_seconds=sleep_seconds,
            write_workers=4,
        )
        stats["symbols"] += len(covered)
        rows = fetch_all(
            "select symbol from public.stock_hist_cache where symbol = any(%s) and adjust = 'qfq' and date = %s "
            "and open > 0 and high >= open and high >= close and low > 0 and low <= open and low <= close "
            "and volume >= 0 and close > 0 and high >= low",
            ([f"US:{symbol}" for symbol in symbols], as_of),
        )
    present = {row["symbol"].removeprefix("US:") for row in rows}
    missing = sorted(set(symbols) - present)
    complete = (
        stats["symbols"] == len(symbols)
        and not missing
        and all(n >= 320 for n in benchmarks.values())
    )
    return {
        "as_of": as_of.isoformat(),
        "symbols": len(symbols),
        "symbols_hash": hashlib.sha256(json.dumps(symbols).encode()).hexdigest(),
        "stats": stats,
        "benchmarks": benchmarks,
        "missing_target": missing,
        "status": "completed" if complete else "incomplete",
        "quality_checked": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--trading-days", type=int, default=360)
    parser.add_argument("--batch-size", type=int, default=40)
    parser.add_argument("--sleep-seconds", type=float, default=0.5)
    args = parser.parse_args()
    try:
        result = refresh_research(
            research_symbols(args.symbols),
            args.as_of,
            trading_days=args.trading_days,
            batch_size=args.batch_size,
            sleep_seconds=args.sleep_seconds,
        )
    except (ValueError, RuntimeError, OSError, psycopg.Error) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "as_of": str(args.as_of),
                    "error_type": type(exc).__name__,
                    "quality_checked": False,
                }
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
