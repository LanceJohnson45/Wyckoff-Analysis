# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_cache import get_cache_meta, normalize_hist_df, upsert_cache_data
from integrations.data_source import _fetch_stock_yfinance
from integrations.fetch_a_share_csv import _resolve_us_window
from integrations.us_sp500_universe import (
    diff_symbols,
    fetch_sp500_constituents,
    get_sp500_constituents,
    load_sp500_snapshot,
    save_sp500_snapshot,
    snapshot_path,
)
from utils.trading_clock import resolve_end_calendar_day


DEFAULT_BATCH_SIZE = max(int(os.getenv("US_SP500_BATCH_SIZE", "40")), 1)
DEFAULT_MAX_WORKERS = max(int(os.getenv("US_SP500_MAX_WORKERS", "4")), 1)
DEFAULT_REFRESH_TRADING_DAYS = max(int(os.getenv("US_REFRESH_TRADING_DAYS", "7")), 1)
DEFAULT_BOOTSTRAP_TRADING_DAYS = max(
    int(os.getenv("US_BOOTSTRAP_TRADING_DAYS", "360")), 30
)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _normalize_batch_download(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    if isinstance(work.columns, pd.MultiIndex):
        level0 = set(str(x) for x in work.columns.get_level_values(0))
        level_last = set(str(x) for x in work.columns.get_level_values(-1))
        if symbol in level0:
            work = work.xs(symbol, axis=1, level=0)
        elif symbol in level_last:
            work = work.xs(symbol, axis=1, level=-1)
        else:
            return pd.DataFrame()
    work = work.reset_index()
    date_col = (
        "Date"
        if "Date" in work.columns
        else ("index" if "index" in work.columns else None)
    )
    if date_col is None:
        return pd.DataFrame()
    work = work.rename(
        columns={
            date_col: "日期",
            "Open": "开盘",
            "High": "最高",
            "Low": "最低",
            "Close": "收盘",
            "Volume": "成交量",
        }
    )
    required = ["日期", "开盘", "最高", "最低", "收盘", "成交量"]
    if any(col not in work.columns for col in required):
        return pd.DataFrame()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ["开盘", "最高", "最低", "收盘", "成交量"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["日期", "收盘"]).copy()
    if work.empty:
        return pd.DataFrame()
    work["成交额"] = pd.to_numeric(work["收盘"], errors="coerce") * pd.to_numeric(
        work["成交量"], errors="coerce"
    )
    work["涨跌幅"] = pd.to_numeric(work["收盘"], errors="coerce").pct_change() * 100.0
    work["涨跌幅"].iloc[0] = 0.0
    work["换手率"] = pd.NA
    base = pd.to_numeric(work["收盘"], errors="coerce").shift(1)
    work["振幅"] = (
        (
            pd.to_numeric(work["最高"], errors="coerce")
            - pd.to_numeric(work["最低"], errors="coerce")
        )
        / base.replace(0, pd.NA)
        * 100.0
    )
    work["振幅"].iloc[0] = (
        (work["最高"].iloc[0] - work["最低"].iloc[0]) / work["开盘"].iloc[0] * 100.0
    )
    return work[
        [
            "日期",
            "开盘",
            "最高",
            "最低",
            "收盘",
            "成交量",
            "成交额",
            "涨跌幅",
            "换手率",
            "振幅",
        ]
    ].copy()


def _download_batch(
    symbols: list[str], start_day: date, end_day: date
) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}
    try:
        import yfinance as yf
    except Exception as e:
        raise RuntimeError(f"yfinance unavailable: {e}") from e
    start_s = start_day.isoformat()
    end_s = (pd.Timestamp(end_day) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    joined = " ".join(symbols)
    data = yf.download(
        joined,
        start=start_s,
        end=end_s,
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=True,
        group_by="ticker",
    )
    if data is None or data.empty:
        raise RuntimeError(f"yfinance batch empty for {joined}")
    out: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        frame = _normalize_batch_download(data, symbol)
        if frame.empty:
            try:
                frame = _fetch_stock_yfinance(
                    symbol, start_day.strftime("%Y%m%d"), end_day.strftime("%Y%m%d")
                )
            except Exception:
                frame = pd.DataFrame()
        if not frame.empty:
            out[symbol] = frame
    return out


def _upsert_us_history(
    symbol: str, df: pd.DataFrame, *, context: str = "background"
) -> int:
    if df is None or df.empty:
        return 0
    norm = normalize_hist_df(df)
    if norm.empty:
        return 0
    latest_date = pd.to_datetime(norm["date"], errors="coerce").dropna().max()
    if pd.isna(latest_date):
        return 0
    
    max_retries = 3
    ok = False
    for attempt in range(max_retries):
        ok = upsert_cache_data(
            symbol=f"US:{symbol}",
            adjust="qfq",
            source="yfinance_batch",
            df=norm,
            context=context,
        )
        if ok:
            break
        if attempt < max_retries - 1:
            time.sleep(0.5 * (attempt + 1))
    
    if not ok:
        return 0
    
    for delay in [0.2, 0.5, 1.0]:
        time.sleep(delay)
        meta = get_cache_meta(f"US:{symbol}", "qfq", context=context)
        if meta is not None and meta.end_date >= latest_date.date():
            return int(len(norm))
    
    return 0


def _latest_cached_date(symbol: str) -> date | None:
    meta = get_cache_meta(f"US:{symbol}", "qfq", context="background")
    return meta.end_date if meta else None


def _filter_symbols_needing_backfill(
    symbols: list[str], *, min_trading_days: int
) -> tuple[list[str], list[str]]:
    end_day = resolve_end_calendar_day()
    target_window = _resolve_us_window(
        end_calendar_day=end_day, trading_days=min_trading_days
    )
    needs: list[str] = []
    covered: list[str] = []
    for symbol in symbols:
        latest = _latest_cached_date(symbol)
        if latest is None or latest < target_window.end_trade_date:
            needs.append(symbol)
        else:
            covered.append(symbol)
    return needs, covered


def _run_batches(
    symbols: list[str],
    *,
    start_day: date,
    end_day: date,
    batch_size: int,
    sleep_seconds: float,
) -> dict[str, int]:
    stats = {"symbols": 0, "rows": 0, "failed": 0, "write_fail": 0}
    for idx, chunk in enumerate(_chunked(symbols, batch_size), start=1):
        _log(f"batch {idx}: downloading {len(chunk)} symbols {start_day}..{end_day}")
        try:
            frames = _download_batch(chunk, start_day, end_day)
        except Exception as e:
            _log(f"batch {idx}: failed: {e}")
            stats["failed"] += len(chunk)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            continue
        for symbol in chunk:
            frame = frames.get(symbol)
            if frame is None or frame.empty:
                stats["failed"] += 1
                _log(f"batch {idx}: empty {symbol}")
                continue
            rows = _upsert_us_history(symbol, frame)
            if rows <= 0:
                stats["write_fail"] += 1
                _log(f"batch {idx}: write verification failed {symbol}")
                continue
            stats["symbols"] += 1
            stats["rows"] += rows
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return stats


def sync_constituents(
    *,
    bootstrap_new: bool,
    bootstrap_trading_days: int,
    batch_size: int,
    sleep_seconds: float,
) -> int:
    previous = load_sp500_snapshot()
    current = fetch_sp500_constituents()
    added, removed = diff_symbols(previous.symbols if previous else [], current.symbols)
    saved = save_sp500_snapshot(current.symbols, source=current.source)
    _log(
        f"sync complete as_of={saved.as_of} total={len(saved.symbols)} added={len(added)} removed={len(removed)} snapshot={snapshot_path()}"
    )
    if added:
        _log(f"added: {', '.join(added[:20])}{'...' if len(added) > 20 else ''}")
    if removed:
        _log(f"removed: {', '.join(removed[:20])}{'...' if len(removed) > 20 else ''}")
    if bootstrap_new and added:
        end_day = resolve_end_calendar_day()
        window = _resolve_us_window(
            end_calendar_day=end_day, trading_days=bootstrap_trading_days
        )
        stats = _run_batches(
            added,
            start_day=window.start_trade_date,
            end_day=window.end_trade_date,
            batch_size=batch_size,
            sleep_seconds=sleep_seconds,
        )
        _log(f"bootstrap_new complete stats={json.dumps(stats, ensure_ascii=False)}")
        if stats["symbols"] <= 0:
            return 1
    return 0


def bootstrap_constituents(
    *, trading_days: int, batch_size: int, sleep_seconds: float
) -> int:
    snapshot = get_sp500_constituents(prefer_snapshot=False)
    save_sp500_snapshot(snapshot.symbols, source=snapshot.source)
    end_day = resolve_end_calendar_day()
    window = _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
    stats = _run_batches(
        snapshot.symbols,
        start_day=window.start_trade_date,
        end_day=window.end_trade_date,
        batch_size=batch_size,
        sleep_seconds=sleep_seconds,
    )
    _log(f"bootstrap complete stats={json.dumps(stats, ensure_ascii=False)}")
    return 0 if stats["symbols"] > 0 else 1


def refresh_constituents(
    *, trading_days: int, batch_size: int, sleep_seconds: float
) -> int:
    snapshot = get_sp500_constituents(prefer_snapshot=False)
    save_sp500_snapshot(snapshot.symbols, source=snapshot.source)
    end_day = resolve_end_calendar_day()
    window = _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
    stats = _run_batches(
        snapshot.symbols,
        start_day=window.start_trade_date,
        end_day=window.end_trade_date,
        batch_size=batch_size,
        sleep_seconds=sleep_seconds,
    )
    _log(f"refresh complete stats={json.dumps(stats, ensure_ascii=False)}")
    return 0 if stats["failed"] < len(snapshot.symbols) else 1


def prewarm_constituents(
    *, trading_days: int, batch_size: int, sleep_seconds: float
) -> int:
    snapshot = get_sp500_constituents(prefer_snapshot=True)
    needs, covered = _filter_symbols_needing_backfill(
        snapshot.symbols, min_trading_days=trading_days
    )
    _log(
        f"prewarm symbols_total={len(snapshot.symbols)} cache_ready={len(covered)} needs_fill={len(needs)}"
    )
    if not needs:
        return 0
    end_day = resolve_end_calendar_day()
    window = _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
    stats = _run_batches(
        needs,
        start_day=window.start_trade_date,
        end_day=window.end_trade_date,
        batch_size=batch_size,
        sleep_seconds=sleep_seconds,
    )
    _log(f"prewarm complete stats={json.dumps(stats, ensure_ascii=False)}")
    return 0 if stats["symbols"] > 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="US S&P500 universe and history maintenance"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sync_parser = sub.add_parser(
        "sync", help="Sync S&P500 constituents and optionally bootstrap new members"
    )
    sync_parser.add_argument("--bootstrap-new", action="store_true")
    sync_parser.add_argument(
        "--bootstrap-trading-days", type=int, default=DEFAULT_BOOTSTRAP_TRADING_DAYS
    )
    sync_parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    sync_parser.add_argument("--sleep-seconds", type=float, default=1.0)

    bootstrap_parser = sub.add_parser(
        "bootstrap", help="Bootstrap full S&P500 history into stock_hist_cache"
    )
    bootstrap_parser.add_argument(
        "--trading-days", type=int, default=DEFAULT_BOOTSTRAP_TRADING_DAYS
    )
    bootstrap_parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    bootstrap_parser.add_argument("--sleep-seconds", type=float, default=1.0)

    refresh_parser = sub.add_parser(
        "refresh", help="Refresh active S&P500 symbols with a short trailing window"
    )
    refresh_parser.add_argument(
        "--trading-days", type=int, default=DEFAULT_REFRESH_TRADING_DAYS
    )
    refresh_parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    refresh_parser.add_argument("--sleep-seconds", type=float, default=0.5)

    prewarm_parser = sub.add_parser(
        "prewarm", help="Ensure active S&P500 symbols have the funnel window cached"
    )
    prewarm_parser.add_argument(
        "--trading-days",
        type=int,
        default=max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30),
    )
    prewarm_parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    prewarm_parser.add_argument("--sleep-seconds", type=float, default=0.5)

    args = parser.parse_args()
    if args.command == "sync":
        return sync_constituents(
            bootstrap_new=bool(args.bootstrap_new),
            bootstrap_trading_days=max(int(args.bootstrap_trading_days), 30),
            batch_size=max(int(args.batch_size), 1),
            sleep_seconds=max(float(args.sleep_seconds), 0.0),
        )
    if args.command == "bootstrap":
        return bootstrap_constituents(
            trading_days=max(int(args.trading_days), 30),
            batch_size=max(int(args.batch_size), 1),
            sleep_seconds=max(float(args.sleep_seconds), 0.0),
        )
    if args.command == "refresh":
        return refresh_constituents(
            trading_days=max(int(args.trading_days), 1),
            batch_size=max(int(args.batch_size), 1),
            sleep_seconds=max(float(args.sleep_seconds), 0.0),
        )
    return prewarm_constituents(
        trading_days=max(int(args.trading_days), 30),
        batch_size=max(int(args.batch_size), 1),
        sleep_seconds=max(float(args.sleep_seconds), 0.0),
    )


if __name__ == "__main__":
    raise SystemExit(main())
