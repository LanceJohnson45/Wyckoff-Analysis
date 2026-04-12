# -*- coding: utf-8 -*-
from __future__ import annotations

from integrations import supabase_market_signal as market_signal


class TestSelectMarketSignalRow:
    def test_prefers_exact_market_match(self):
        rows = [
            {"trade_date": "2026-04-11", "market": "cn", "benchmark_regime": "NEUTRAL"},
            {"trade_date": "2026-04-11", "market": "us", "benchmark_regime": "RISK_OFF"},
        ]

        result = market_signal._select_market_signal_row(rows, "us")

        assert result is not None
        assert result["market"] == "us"
        assert result["benchmark_regime"] == "RISK_OFF"

    def test_cn_falls_back_to_legacy_row_without_market(self):
        rows = [
            {"trade_date": "2026-04-11", "benchmark_regime": "NEUTRAL"},
            {"trade_date": "2026-04-11", "market": "us", "benchmark_regime": "RISK_OFF"},
        ]

        result = market_signal._select_market_signal_row(rows, "cn")

        assert result is not None
        assert result.get("market") in {None, ""}
        assert result["benchmark_regime"] == "NEUTRAL"


class TestNormalizeRowForUpsert:
    def test_normalizes_market_field(self):
        result = market_signal._normalize_row_for_upsert({"market": "US", "trade_date": "2026-04-11"})

        assert result["market"] == "us"
