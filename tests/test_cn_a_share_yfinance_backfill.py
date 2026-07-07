# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date

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
