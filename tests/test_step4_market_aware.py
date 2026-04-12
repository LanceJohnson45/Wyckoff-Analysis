# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime

from scripts import step4_rebalancer as step4


class TestStep4MarketAwareHelpers:
    def test_job_end_calendar_day_uses_us_timezone(self):
        now = datetime(2026, 4, 10, 15, 0, tzinfo=step4.US_TZ)

        assert step4._job_end_calendar_day("us", now) == now.date().replace(day=9)

    def test_load_market_signal_passes_market_dimension(self, monkeypatch):
        captured: dict[str, str] = {}

        def fake_loader(trade_date, market="cn", client=None):
            captured["trade_date"] = trade_date
            captured["market"] = market
            return {"trade_date": trade_date, "market": market}

        monkeypatch.setattr(step4, "load_market_signal_daily", fake_loader)

        result = step4._load_market_signal_for_trade_date("2026-04-11", "us")

        assert result == {"trade_date": "2026-04-11", "market": "us"}
        assert captured == {"trade_date": "2026-04-11", "market": "us"}
