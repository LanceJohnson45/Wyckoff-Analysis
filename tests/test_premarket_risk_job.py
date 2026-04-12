# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime

from scripts import premarket_risk_job as risk_job


class TestPremarketTradeDate:
    def test_cn_trade_date_uses_shanghai_day(self):
        now = datetime(2026, 4, 11, 7, 0, tzinfo=risk_job.TZ)

        assert risk_job._premarket_session_trade_date_str_for_market("cn", now) == "2026-04-11"

    def test_us_trade_date_uses_same_weekday_before_close(self):
        now = datetime(2026, 4, 10, 8, 0, tzinfo=risk_job.US_TZ)

        assert risk_job._premarket_session_trade_date_str_for_market("us", now) == "2026-04-10"

    def test_us_trade_date_rolls_to_next_weekday_after_close(self):
        now = datetime(2026, 4, 10, 17, 30, tzinfo=risk_job.US_TZ)

        assert risk_job._premarket_session_trade_date_str_for_market("us", now) == "2026-04-13"


class TestJudgeUsRegime:
    def test_black_swan_when_vix_spikes_with_high_absolute_level(self):
        regime, reasons = risk_job._judge_us_regime({"close": 28.0, "pct_chg": 18.0})

        assert regime == "BLACK_SWAN"
        assert reasons

    def test_normal_when_vix_does_not_trigger_threshold(self):
        regime, reasons = risk_job._judge_us_regime({"close": 16.0, "pct_chg": 2.0})

        assert regime == "NORMAL"
        assert "VIX" in reasons[0]
