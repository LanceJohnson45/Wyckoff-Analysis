from __future__ import annotations

from integrations.us_sp500_universe import _normalize_yahoo_symbol, diff_symbols


def test_normalize_yahoo_symbol_rewrites_dot_share_classes():
    assert _normalize_yahoo_symbol("BRK.B") == "BRK-B"
    assert _normalize_yahoo_symbol("bf.b") == "BF-B"


def test_diff_symbols_reports_added_and_removed():
    added, removed = diff_symbols(["AAPL", "MSFT"], ["MSFT", "NVDA"])
    assert added == ["NVDA"]
    assert removed == ["AAPL"]
