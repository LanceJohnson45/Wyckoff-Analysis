# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date

import pandas as pd

from scripts import us_yfinance_backfill as mod


def test_symbol_helpers_normalize_us_cache_keys():
    assert mod._cache_symbol("aapl") == "US:AAPL"
    assert mod._cache_symbol("US:msft") == "US:MSFT"
    assert mod._raw_symbol("US:nvda") == "NVDA"
    assert mod._parse_symbols("aapl, US:msft\naapl") == ["AAPL", "MSFT"]


def test_missing_ranges_returns_discrete_windows():
    expected = [
        date(2025, 7, 7),
        date(2025, 7, 8),
        date(2025, 7, 9),
        date(2025, 7, 10),
        date(2025, 7, 11),
    ]
    cached = [
        date(2025, 7, 7),
        date(2025, 7, 9),
        date(2025, 7, 11),
    ]

    assert mod._missing_ranges(expected, cached) == [
        (date(2025, 7, 8), date(2025, 7, 8)),
        (date(2025, 7, 10), date(2025, 7, 10)),
    ]


def test_split_gap_ranges_breaks_long_windows():
    ranges = [(date(2025, 7, 7), date(2025, 7, 16))]

    assert mod._split_gap_ranges(ranges, max_span_days=4) == [
        (date(2025, 7, 7), date(2025, 7, 10)),
        (date(2025, 7, 11), date(2025, 7, 14)),
        (date(2025, 7, 15), date(2025, 7, 16)),
    ]


def test_resolve_target_window_excludes_us_market_holiday():
    start_day, end_day, trade_dates = mod._resolve_target_window(
        start_date="2025-07-03",
        end_date="2025-07-07",
        lookback_days=30,
    )

    assert start_day == date(2025, 7, 3)
    assert end_day == date(2025, 7, 7)
    assert trade_dates == [date(2025, 7, 3), date(2025, 7, 7)]


def test_normalize_batch_download_extracts_symbol_frame():
    dates = pd.to_datetime(["2025-07-07", "2025-07-08"])
    data = pd.DataFrame(
        {
            ("AAPL", "Open"): [10.0, 10.2],
            ("AAPL", "High"): [10.5, 10.4],
            ("AAPL", "Low"): [9.8, 10.0],
            ("AAPL", "Close"): [10.2, 10.3],
            ("AAPL", "Volume"): [1000, 1100],
            ("MSFT", "Open"): [100.0, 101.0],
            ("MSFT", "High"): [102.0, 103.0],
            ("MSFT", "Low"): [99.0, 100.0],
            ("MSFT", "Close"): [101.0, 102.0],
            ("MSFT", "Volume"): [2000, 2100],
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )

    frame = mod._normalize_batch_download(
        data,
        "AAPL",
        start_day=date(2025, 7, 7),
        end_day=date(2025, 7, 8),
    )

    assert list(frame["日期"]) == ["2025-07-07", "2025-07-08"]
    assert list(frame["收盘"]) == [10.2, 10.3]
    assert list(frame["成交额"]) == [10200.0, 11330.0]


def test_run_batch_backfill_downloads_once_for_many_symbols(monkeypatch):
    expected = [date(2025, 7, 7), date(2025, 7, 8)]
    written: set[str] = set()

    def fake_cached_dates(symbol, *args, **kwargs):
        return expected if symbol in written else []

    monkeypatch.setattr(mod, "load_cached_dates", fake_cached_dates)
    monkeypatch.setattr(mod, "postgres_enabled", lambda: False)
    calls = []

    def fake_download(symbols, *, start_day, end_day, yf_threads=1):
        calls.append(list(symbols))
        return {
            symbol: pd.DataFrame(
                {
                    "日期": ["2025-07-07", "2025-07-08"],
                    "开盘": [10.0, 10.2],
                    "最高": [10.4, 10.5],
                    "最低": [9.9, 10.0],
                    "收盘": [10.2, 10.3],
                    "成交量": [1000, 1100],
                    "成交额": [10200, 11330],
                    "涨跌幅": [0.0, 0.98],
                    "换手率": [pd.NA, pd.NA],
                    "振幅": [pd.NA, 4.9],
                }
            )
            for symbol in symbols
        }

    def fake_upsert(symbol, frame):
        written.add(mod._cache_symbol(symbol))
        return len(frame)

    monkeypatch.setattr(mod, "_download_batch", fake_download)
    monkeypatch.setattr(mod, "_upsert_symbol_range", fake_upsert)

    stats = mod._run_batch_backfill(
        ["AAPL", "MSFT", "NVDA"],
        start_day=date(2025, 7, 7),
        end_day=date(2025, 7, 8),
        expected_dates=expected,
        batch_size=2,
        sleep_seconds=0.0,
        jitter_seconds=0.0,
        progress_every=1,
    )

    assert calls == [["AAPL", "MSFT"], ["NVDA"]]
    assert stats["symbols_updated"] == 3
    assert stats["rows_written"] == 6
    assert written == {"US:AAPL", "US:MSFT", "US:NVDA"}


def test_run_batch_backfill_reports_cache_ready_when_no_download_needed(monkeypatch):
    expected = [date(2025, 7, 7), date(2025, 7, 8)]
    monkeypatch.setattr(mod, "postgres_enabled", lambda: False)
    monkeypatch.setattr(mod, "load_cached_dates", lambda *args, **kwargs: expected)

    calls = []
    monkeypatch.setattr(
        mod,
        "_download_batch",
        lambda *args, **kwargs: calls.append(args),
    )

    stats = mod._run_batch_backfill(
        ["AAPL", "MSFT", "NVDA"],
        start_day=date(2025, 7, 7),
        end_day=date(2025, 7, 8),
        expected_dates=expected,
        batch_size=2,
        sleep_seconds=0.0,
        jitter_seconds=0.0,
        progress_every=1,
    )

    assert calls == []
    assert stats["symbols_cache_ready"] == 3
    assert stats["symbols_need_update"] == 0
