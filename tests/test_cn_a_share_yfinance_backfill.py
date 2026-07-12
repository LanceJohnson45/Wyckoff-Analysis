# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date

import pandas as pd

from scripts import cn_a_share_yfinance_backfill as mod


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


def test_load_supported_symbols_filters_non_yfinance_codes(monkeypatch):
    monkeypatch.setattr(
        mod,
        "get_all_stocks",
        lambda: [
            {"code": "600519", "name": "茅台"},
            {"code": "000001", "name": "平安银行"},
            {"code": "430001", "name": "北交所示例"},
            {"code": "600519", "name": "重复"},
        ],
    )

    symbols, unsupported = mod._load_supported_symbols()

    assert symbols == ["000001", "600519"]
    assert unsupported == 1


def test_normalize_batch_download_extracts_symbol_frame():
    dates = pd.to_datetime(["2025-07-07", "2025-07-08"])
    data = pd.DataFrame(
        {
            ("000001.SZ", "Open"): [10.0, 10.2],
            ("000001.SZ", "High"): [10.5, 10.4],
            ("000001.SZ", "Low"): [9.8, 10.0],
            ("000001.SZ", "Close"): [10.2, 10.3],
            ("000001.SZ", "Volume"): [1000, 1100],
            ("600519.SS", "Open"): [100.0, 101.0],
            ("600519.SS", "High"): [102.0, 103.0],
            ("600519.SS", "Low"): [99.0, 100.0],
            ("600519.SS", "Close"): [101.0, 102.0],
            ("600519.SS", "Volume"): [2000, 2100],
        },
        index=pd.DatetimeIndex(dates, name="Date"),
    )

    frame = mod._normalize_batch_download(
        data,
        "000001.SZ",
        start_day=date(2025, 7, 7),
        end_day=date(2025, 7, 8),
    )

    assert list(frame["日期"]) == ["2025-07-07", "2025-07-08"]
    assert list(frame["收盘"]) == [10.2, 10.3]


def test_run_batch_backfill_downloads_once_for_many_symbols(monkeypatch):
    expected = [date(2025, 7, 7), date(2025, 7, 8)]
    written = set()

    def fake_cached_dates(symbol, *args, **kwargs):
        return expected if symbol in written else []

    monkeypatch.setattr(
        mod,
        "load_cached_dates",
        fake_cached_dates,
    )
    monkeypatch.setattr(
        mod,
        "_cn_stock_to_yfinance_symbol",
        lambda symbol: f"{symbol}.SZ",
    )
    calls = []

    def fake_download(yf_symbols, *, start_day, end_day):
        calls.append(list(yf_symbols))
        return {
            yf_symbol: pd.DataFrame(
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
            for yf_symbol in yf_symbols
        }

    monkeypatch.setattr(mod, "_download_batch", fake_download)
    def fake_upsert(symbol, frame):
        written.add(symbol)
        return len(frame)

    monkeypatch.setattr(mod, "_upsert_symbol_range", fake_upsert)

    stats = mod._run_batch_backfill(
        ["000001", "000002", "000003"],
        start_day=date(2025, 7, 7),
        end_day=date(2025, 7, 8),
        expected_dates=expected,
        batch_size=2,
        sleep_seconds=0.0,
        jitter_seconds=0.0,
        progress_every=1,
    )

    assert calls == [["000001.SZ", "000002.SZ"], ["000003.SZ"]]
    assert stats["symbols_updated"] == 3
    assert stats["rows_written"] == 6
