# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections.abc import Iterable
from datetime import date, datetime, timedelta

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

from core.constants import TABLE_STOCK_HIST_CACHE
from core.stock_cache import load_cached_dates, normalize_hist_df, upsert_cache_data
from integrations.data_source import _fetch_stock_yfinance
from integrations.postgres_base import connect_postgres, postgres_enabled
from integrations.us_sp500_universe import get_sp500_constituents
from utils.trading_clock import resolve_end_calendar_day_for_market


DEFAULT_LOOKBACK_DAYS = max(int(os.getenv("US_BACKFILL_LOOKBACK_DAYS", "365")), 30)
DEFAULT_SLEEP_SECONDS = max(float(os.getenv("US_BACKFILL_SLEEP_SECONDS", "0.5")), 0.0)
DEFAULT_JITTER_SECONDS = max(float(os.getenv("US_BACKFILL_JITTER_SECONDS", "0.2")), 0.0)
DEFAULT_RETRY_TIMES = max(int(os.getenv("US_BACKFILL_RETRY_TIMES", "3")), 1)
DEFAULT_RETRY_BACKOFF_SECONDS = max(float(os.getenv("US_BACKFILL_RETRY_BACKOFF_SECONDS", "3.0")), 0.0)
DEFAULT_GAP_CHUNK_DAYS = max(int(os.getenv("US_BACKFILL_GAP_CHUNK_DAYS", "160")), 10)
DEFAULT_PROGRESS_EVERY = max(int(os.getenv("US_BACKFILL_PROGRESS_EVERY", "25")), 1)
DEFAULT_BATCH_SIZE = max(int(os.getenv("US_BACKFILL_BATCH_SIZE", "10")), 1)
DEFAULT_YF_THREADS = max(int(os.getenv("US_BACKFILL_YF_THREADS", "1")), 0)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _cache_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    return text if text.startswith("US:") else f"US:{text}"


def _raw_symbol(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    return text[3:] if text.startswith("US:") else text


def _parse_symbols(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in str(raw or "").replace("\n", ",").split(","):
        symbol = _raw_symbol(item).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        out.append(symbol)
    return out


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


def _resolve_target_window(
    *,
    start_date: str | None,
    end_date: str | None,
    lookback_days: int,
) -> tuple[date, date, list[date]]:
    if end_date:
        end_day = pd.to_datetime(end_date, errors="coerce").date()
    else:
        end_day = resolve_end_calendar_day_for_market("us")
    if start_date:
        start_day = pd.to_datetime(start_date, errors="coerce").date()
    else:
        start_day = end_day - timedelta(days=max(int(lookback_days), 1))
    if start_day > end_day:
        raise ValueError("start_date must be <= end_date")
    trade_dates = _approx_us_market_dates(start=start_day, end=end_day)
    if not trade_dates:
        raise RuntimeError(f"no US business dates found in requested window {start_day}..{end_day}")
    snapped_start = trade_dates[0]
    snapped_end = trade_dates[-1]
    if snapped_start != start_day or snapped_end != end_day:
        _log(
            f"window snapped from {start_day}..{end_day} "
            f"to US trading window {snapped_start}..{snapped_end}"
        )
    return snapped_start, snapped_end, trade_dates


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
    sleep_for = max(float(base_seconds), 0.0)
    if jitter_seconds > 0:
        sleep_for += random.uniform(0.0, float(jitter_seconds))
    if sleep_for > 0:
        time.sleep(sleep_for)


def _chunked(items: list[object], size: int) -> Iterable[list[object]]:
    size = max(int(size), 1)
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _coerce_cache_date(raw: object) -> date | None:
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    try:
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()
    except Exception:
        return None


def _load_cached_dates_many(
    symbols: list[str],
    *,
    adjust: str,
    start_day: date,
    end_day: date,
) -> dict[str, list[date]]:
    cache_symbols = [_cache_symbol(symbol) for symbol in symbols]
    out: dict[str, set[date]] = {symbol: set() for symbol in symbols}
    cache_to_raw = {_cache_symbol(symbol): symbol for symbol in symbols}
    if not symbols:
        return {}
    if postgres_enabled():
        try:
            with connect_postgres() as conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    select symbol, date
                    from public.{TABLE_STOCK_HIST_CACHE}
                    where symbol = any(%s)
                      and adjust = %s
                      and date >= %s
                      and date <= %s
                    order by symbol, date
                    """,
                    (cache_symbols, adjust, start_day, end_day),
                )
                rows = cur.fetchall()
            for row in rows:
                raw_symbol = cache_to_raw.get(str(row.get("symbol") or ""))
                day = _coerce_cache_date(row.get("date"))
                if raw_symbol in out and day is not None:
                    out[raw_symbol].add(day)
            return {symbol: sorted(days) for symbol, days in out.items()}
        except Exception as e:
            _log(f"bulk cache scan failed, falling back to per-symbol scan: {type(e).__name__}: {e}")
    return {
        symbol: load_cached_dates(
            _cache_symbol(symbol),
            adjust,
            start_day,
            end_day,
            context="background",
        )
        for symbol in symbols
    }


def _load_symbols(*, symbols_arg: str, limit: int) -> list[str]:
    explicit = _parse_symbols(symbols_arg)
    if explicit:
        symbols = explicit
    else:
        snapshot = get_sp500_constituents(prefer_snapshot=True)
        symbols = list(snapshot.symbols)
        _log(f"loaded US universe source={snapshot.source} as_of={snapshot.as_of} symbols={len(symbols)}")
    if limit > 0:
        symbols = symbols[:limit]
    if not symbols:
        raise RuntimeError("no US symbols available for yfinance backfill")
    return symbols


def _normalize_batch_download(
    df: pd.DataFrame,
    symbol: str,
    *,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
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
    date_col = "Date" if "Date" in work.columns else ("index" if "index" in work.columns else None)
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
    start_iso = start_day.isoformat()
    end_iso = end_day.isoformat()
    work = work.loc[(work["日期"] >= start_iso) & (work["日期"] <= end_iso)].copy()
    if work.empty:
        return pd.DataFrame()
    for col in ["开盘", "最高", "最低", "收盘", "成交量"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["日期", "收盘"]).copy()
    if work.empty:
        return pd.DataFrame()
    work["成交额"] = work["收盘"] * work["成交量"]
    work["涨跌幅"] = work["收盘"].pct_change(fill_method=None) * 100.0
    work["换手率"] = pd.NA
    base = work["收盘"].shift(1)
    work["振幅"] = (work["最高"] - work["最低"]) / base.replace(0, pd.NA) * 100.0
    return work[["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率", "振幅"]].copy()


def _download_batch(
    symbols: list[str],
    *,
    start_day: date,
    end_day: date,
    yf_threads: int | bool = DEFAULT_YF_THREADS,
) -> dict[str, pd.DataFrame]:
    if not symbols:
        return {}
    try:
        import yfinance as yf
    except Exception as e:
        raise RuntimeError(f"yfinance unavailable: {e}") from e
    fetch_start = pd.Timestamp(start_day) - pd.Timedelta(days=7)
    fetch_end = pd.Timestamp(end_day) + pd.Timedelta(days=3)
    joined = " ".join(symbols)
    data = yf.download(
        joined,
        start=fetch_start.strftime("%Y-%m-%d"),
        end=fetch_end.strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=yf_threads if yf_threads else False,
        group_by="ticker",
    )
    if data is None or data.empty:
        raise RuntimeError(f"yfinance batch empty for {joined}")
    frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        frame = _normalize_batch_download(data, symbol, start_day=start_day, end_day=end_day)
        if not frame.empty:
            frames[symbol] = frame
    return frames


def _fetch_gap_frame(
    *,
    symbol: str,
    gap_start: date,
    gap_end: date,
    retry_times: int,
    retry_backoff_seconds: float,
) -> pd.DataFrame:
    last_err: Exception | None = None
    for attempt in range(1, retry_times + 1):
        try:
            return _fetch_stock_yfinance(
                symbol,
                gap_start.strftime("%Y%m%d"),
                gap_end.strftime("%Y%m%d"),
            )
        except Exception as e:
            last_err = e
            _log(f"fetch retry symbol={symbol} range={gap_start}..{gap_end} attempt={attempt}/{retry_times} err={type(e).__name__}: {e}")
            if attempt < retry_times:
                time.sleep(retry_backoff_seconds * attempt)
    raise RuntimeError(f"fetch failed for {symbol} {gap_start}..{gap_end}: {last_err}") from last_err


def _upsert_symbol_range(symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    norm = normalize_hist_df(df)
    if norm.empty:
        return 0
    ok = upsert_cache_data(
        symbol=_cache_symbol(symbol),
        adjust="qfq",
        source="yfinance_backfill",
        df=norm,
        context="background",
    )
    return int(len(norm)) if ok else 0


def _iter_backfill_task_batches(
    symbols: list[str],
    *,
    start_day: date,
    end_day: date,
    expected_dates: list[date],
    batch_size: int,
    progress_every: int,
) -> Iterable[tuple[list[dict[str, object]], int, int, int]]:
    chunk: list[dict[str, object]] = []
    cache_ready = 0
    need_update = 0
    total = len(symbols)
    size = max(int(batch_size), 1)
    _log(f"cache scan start symbols={total} window={start_day}..{end_day} stream_batch_size={size}")
    idx = 0
    last_progress_idx = 0
    for symbol_chunk in _chunked(symbols, size):
        raw_symbols = [str(x) for x in symbol_chunk]
        cached_by_symbol = _load_cached_dates_many(
            raw_symbols,
            adjust="qfq",
            start_day=start_day,
            end_day=end_day,
        )
        for symbol in raw_symbols:
            idx += 1
            cached_dates = cached_by_symbol.get(symbol, [])
            gap_ranges = _missing_ranges(expected_dates, cached_dates)
            if not gap_ranges:
                cache_ready += 1
            else:
                need_update += 1
                chunk.append({"symbol": symbol, "missing_dates": len(set(expected_dates) - set(cached_dates))})
        if idx - last_progress_idx >= max(int(progress_every), 1) or idx >= total:
            _log(f"cache scan progress {idx}/{total} cache_ready={cache_ready} need_update={need_update} pending_batch={len(chunk)}")
            last_progress_idx = idx
        if len(chunk) >= size:
            yield chunk, idx, cache_ready, need_update
            chunk = []
    if chunk:
        yield chunk, total, cache_ready, need_update
    else:
        yield [], total, cache_ready, need_update


def _run_batch_backfill(
    symbols: list[str],
    *,
    start_day: date,
    end_day: date,
    expected_dates: list[date],
    batch_size: int,
    sleep_seconds: float,
    jitter_seconds: float,
    progress_every: int,
    yf_threads: int | bool = DEFAULT_YF_THREADS,
) -> dict[str, object]:
    stats: dict[str, object] = {
        "symbols_total": len(symbols),
        "symbols_cache_ready": 0,
        "symbols_updated": 0,
        "symbols_failed": 0,
        "rows_written": 0,
        "failed_symbols": [],
        "download_batches": 0,
        "download_batch_size": max(int(batch_size), 1),
        "symbols_need_update": 0,
    }
    _log(f"batch mode: streaming cache scan and download batch_size={batch_size}")
    batch_no = 0
    last_scanned = 0
    for chunk, scanned, cache_ready, need_update in _iter_backfill_task_batches(
        symbols,
        start_day=start_day,
        end_day=end_day,
        expected_dates=expected_dates,
        batch_size=batch_size,
        progress_every=progress_every,
    ):
        stats["symbols_cache_ready"] = cache_ready
        stats["symbols_need_update"] = need_update
        last_scanned = scanned
        if not chunk:
            continue
        batch_no += 1
        batch_symbols = [str(item["symbol"]) for item in chunk]
        _log(f"batch {batch_no}: downloading {len(batch_symbols)} symbols scanned={scanned}/{len(symbols)} cache_ready={cache_ready} need_update={need_update}")
        try:
            frames = _download_batch(
                batch_symbols,
                start_day=start_day,
                end_day=end_day,
                yf_threads=yf_threads,
            )
            stats["download_batches"] = int(stats["download_batches"]) + 1
        except Exception as e:
            _log(f"batch {batch_no}: download failed: {type(e).__name__}: {e}")
            for item in chunk:
                stats["symbols_failed"] = int(stats["symbols_failed"]) + 1
                stats["failed_symbols"].append({"symbol": str(item["symbol"]), "error": str(e)})
            _sleep_between_symbols(sleep_seconds, jitter_seconds)
            continue
        for symbol in batch_symbols:
            frame = frames.get(symbol)
            if frame is None or frame.empty:
                try:
                    frame = _fetch_gap_frame(
                        symbol=symbol,
                        gap_start=start_day,
                        gap_end=end_day,
                        retry_times=2,
                        retry_backoff_seconds=2.0,
                    )
                except Exception as e:
                    stats["symbols_failed"] = int(stats["symbols_failed"]) + 1
                    stats["failed_symbols"].append({"symbol": symbol, "error": f"empty batch fallback failed: {e}"})
                    continue
            try:
                rows = _upsert_symbol_range(symbol, frame)
                if rows <= 0:
                    raise RuntimeError("upsert returned 0 rows")
                refreshed_dates = load_cached_dates(
                    _cache_symbol(symbol),
                    "qfq",
                    start_day,
                    end_day,
                    context="background",
                )
                still_missing = sorted(set(expected_dates) - set(refreshed_dates))
                if still_missing:
                    raise RuntimeError(f"write verify missing {len(still_missing)} dates first_missing={still_missing[:5]}")
                stats["symbols_updated"] = int(stats["symbols_updated"]) + 1
                stats["rows_written"] = int(stats["rows_written"]) + int(rows)
            except Exception as e:
                stats["symbols_failed"] = int(stats["symbols_failed"]) + 1
                stats["failed_symbols"].append({"symbol": symbol, "error": str(e)})
        _log(f"batch progress batches={batch_no} scanned={scanned}/{len(symbols)} updated={stats['symbols_updated']} failed={stats['symbols_failed']} rows={stats['rows_written']}")
        if scanned < len(symbols):
            _sleep_between_symbols(sleep_seconds, jitter_seconds)
    _log(f"batch mode scan done scanned={last_scanned}/{len(symbols)} cache_ready={stats['symbols_cache_ready']} need_update={stats['symbols_need_update']} batches={batch_no}")
    return stats


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
    cached_dates = load_cached_dates(_cache_symbol(symbol), "qfq", start_day, end_day, context="background")
    gap_ranges = _missing_ranges(expected_dates, cached_dates)
    if not gap_ranges:
        return {"symbol": symbol, "status": "cache_ready", "rows": 0, "ranges": [], "missing_dates": 0}
    split_ranges = _split_gap_ranges(gap_ranges, max_span_days=gap_chunk_days)
    written_rows = 0
    for gap_start, gap_end in split_ranges:
        frame = _fetch_gap_frame(
            symbol=symbol,
            gap_start=gap_start,
            gap_end=gap_end,
            retry_times=retry_times,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        rows = _upsert_symbol_range(symbol, frame)
        if rows <= 0:
            raise RuntimeError(f"upsert failed for {symbol} {gap_start}..{gap_end}")
        written_rows += rows
    refreshed_dates = load_cached_dates(_cache_symbol(symbol), "qfq", start_day, end_day, context="background")
    still_missing = sorted(set(expected_dates) - set(refreshed_dates))
    if still_missing:
        raise RuntimeError(f"write verify failed for {symbol}, missing {len(still_missing)} dates, first_missing={still_missing[:5]}")
    return {
        "symbol": symbol,
        "status": "updated",
        "rows": written_rows,
        "ranges": [(a.isoformat(), b.isoformat()) for a, b in split_ranges],
        "missing_dates": len(set(expected_dates) - set(cached_dates)),
    }


def run_backfill(args: argparse.Namespace) -> int:
    start_day, end_day, expected_dates = _resolve_target_window(
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=max(int(args.lookback_days), 1),
    )
    symbols = _load_symbols(symbols_arg=args.symbols, limit=max(int(args.limit), 0))
    stats = {
        "symbols_total": len(symbols),
        "symbols_cache_ready": 0,
        "symbols_updated": 0,
        "symbols_failed": 0,
        "rows_written": 0,
        "expected_trade_dates": len(expected_dates),
        "failed_symbols": [],
    }
    _log(
        "us yfinance backfill start "
        f"symbols={len(symbols)} window={start_day}..{end_day} business_dates={len(expected_dates)} "
        f"sleep={args.sleep_seconds}s jitter={args.jitter_seconds}s "
        f"gap_chunk_days={args.gap_chunk_days} batch_size={args.batch_size} yf_threads={args.yf_threads}"
    )
    if int(args.batch_size) > 1:
        batch_stats = _run_batch_backfill(
            symbols,
            start_day=start_day,
            end_day=end_day,
            expected_dates=expected_dates,
            batch_size=max(int(args.batch_size), 1),
            sleep_seconds=float(args.sleep_seconds),
            jitter_seconds=float(args.jitter_seconds),
            progress_every=max(int(args.progress_every), 1),
            yf_threads=max(int(args.yf_threads), 0),
        )
        stats.update(batch_stats)
        stats["expected_trade_dates"] = len(expected_dates)
        _log(f"us yfinance backfill done {json.dumps(stats, ensure_ascii=False)}")
        return 0 if (int(stats["symbols_updated"]) > 0 or int(stats["symbols_cache_ready"]) > 0) else 1
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
                _log(f"updated {symbol} rows={result.get('rows', 0)} ranges={result.get('ranges', [])}")
        except Exception as e:
            stats["symbols_failed"] += 1
            stats["failed_symbols"].append({"symbol": symbol, "error": str(e)})
            _log(f"failed {symbol}: {type(e).__name__}: {e}")
        if idx % max(int(args.progress_every), 1) == 0 or idx == len(symbols):
            _log(f"progress {idx}/{len(symbols)} cache_ready={stats['symbols_cache_ready']} updated={stats['symbols_updated']} failed={stats['symbols_failed']} rows={stats['rows_written']}")
        if idx < len(symbols):
            _sleep_between_symbols(float(args.sleep_seconds), float(args.jitter_seconds))
    _log(f"us yfinance backfill done {json.dumps(stats, ensure_ascii=False)}")
    return 0 if (stats["symbols_updated"] > 0 or stats["symbols_cache_ready"] > 0) else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill US daily bars into stock_hist_cache via yfinance")
    parser.add_argument("--start-date", default=os.getenv("US_BACKFILL_START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("US_BACKFILL_END_DATE", ""))
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--jitter-seconds", type=float, default=DEFAULT_JITTER_SECONDS)
    parser.add_argument("--retry-times", type=int, default=DEFAULT_RETRY_TIMES)
    parser.add_argument("--retry-backoff-seconds", type=float, default=DEFAULT_RETRY_BACKOFF_SECONDS)
    parser.add_argument("--gap-chunk-days", type=int, default=DEFAULT_GAP_CHUNK_DAYS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--yf-threads",
        type=int,
        default=DEFAULT_YF_THREADS,
        help="yfinance worker threads per batch; 0 disables threading. Default 1 to avoid Yahoo rate limits",
    )
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY)
    parser.add_argument("--limit", type=int, default=max(int(os.getenv("US_BACKFILL_LIMIT", "0")), 0))
    parser.add_argument("--symbols", default=os.getenv("US_BACKFILL_SYMBOLS", ""), help="Comma-separated symbols; default uses the US funnel universe")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_backfill(args)


if __name__ == "__main__":
    raise SystemExit(main())
