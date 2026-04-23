# -*- coding: utf-8 -*-
"""core/wyckoff_engine.py 冒烟测试。"""
from __future__ import annotations

import pandas as pd
import pytest

from core.wyckoff_engine import (
    DataIntegrityPolicy,
    FunnelConfig,
    L2Metrics,
    _latest_trade_date,
    _sorted_if_needed,
    assess_hist_integrity,
    layer1_filter,
    layer2_strength_detailed,
    run_funnel,
    score_track_a,
    score_track_b,
    score_track_c,
    select_l2_decision,
)


def _make_df(dates, closes, volumes=None) -> pd.DataFrame:
    n = len(dates)
    opens = closes
    highs = [c * 1.01 for c in closes]
    lows = [c * 0.99 for c in closes]
    vols = volumes or [1_000_000] * n
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "open": opens,
            "close": closes,
            "high": highs,
            "low": lows,
            "volume": vols,
        }
    )


class TestSortedIfNeeded:
    def test_already_sorted(self):
        df = _make_df(["2024-01-01", "2024-01-02", "2024-01-03"], [10, 11, 12])
        result = _sorted_if_needed(df)
        assert list(result["close"]) == [10, 11, 12]

    def test_reverse_sorted(self):
        df = _make_df(["2024-01-03", "2024-01-02", "2024-01-01"], [12, 11, 10])
        result = _sorted_if_needed(df)
        assert list(result["close"]) == [10, 11, 12]


class TestLatestTradeDate:
    def test_returns_last_date(self):
        df = _make_df(["2024-01-01", "2024-01-02", "2024-01-03"], [10, 11, 12])
        result = _latest_trade_date(df)
        assert pd.Timestamp(result) == pd.Timestamp("2024-01-03")

    def test_empty_df_returns_none(self):
        df = pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
        result = _latest_trade_date(df)
        assert result is None


class TestLayer1Filter:
    def test_filters_st_stocks(self):
        """L1 应剔除 ST 股票（名称含 ST）。"""
        cfg = FunnelConfig()
        # 准备一只正常股和一只 ST 股
        dates = pd.date_range("2024-01-01", periods=100, freq="B")
        closes = [10 + i * 0.01 for i in range(100)]
        df = _make_df(dates.strftime("%Y-%m-%d").tolist(), closes)

        name_map = {"000001": "平安银行", "000002": "ST 万科"}
        # 给足够大的市值和成交额，让非 ST 股通过
        mcap = {"000001": 5e10, "000002": 5e10}
        df_map = {"000001": df.copy(), "000002": df.copy()}

        result = layer1_filter(["000001", "000002"], name_map, mcap, df_map, cfg)
        assert "000002" not in result  # ST 被剔除

    def test_us_symbols_are_not_blocked_by_cn_code_rules(self):
        """US 模式不应套用 A 股代码前缀过滤。"""
        cfg = FunnelConfig()
        dates = pd.date_range("2024-01-01", periods=100, freq="B")
        closes = [100 + i * 0.2 for i in range(100)]
        df = _make_df(dates.strftime("%Y-%m-%d").tolist(), closes)
        df["amount"] = 1_000_000_000.0

        name_map = {"AAPL": "Apple Inc."}
        df_map = {"AAPL": df}

        result = layer1_filter(
            ["AAPL"],
            name_map,
            {},
            df_map,
            cfg,
            market="us",
        )
        assert result == ["AAPL"]

    def test_returns_rejection_reasons_when_requested(self):
        cfg = FunnelConfig()
        dates = pd.date_range("2024-01-01", periods=40, freq="B")
        closes = [10 + i * 0.05 for i in range(40)]
        df = _make_df(dates.strftime("%Y-%m-%d").tolist(), closes)
        df["amount"] = 1_000.0

        passed, rejected = layer1_filter(
            ["000001"],
            {"000001": "平安银行"},
            {"000001": 100.0},
            {"000001": df},
            cfg,
            return_rejections=True,
        )
        assert passed == []
        assert rejected["000001"]["reason"] == "avg_amount_below_threshold"

    def test_partial_market_cap_map_does_not_reject_missing_cap(self):
        cfg = FunnelConfig()
        dates = pd.date_range("2024-01-01", periods=100, freq="B")
        closes = [10 + i * 0.05 for i in range(100)]
        df = _make_df(dates.strftime("%Y-%m-%d").tolist(), closes)
        df["amount"] = 1_000_000_000.0

        result = layer1_filter(
            ["000001", "000002"],
            {"000001": "平安银行", "000002": "万科A"},
            {"000001": 100.0},
            {"000001": df.copy(), "000002": df.copy()},
            cfg,
        )

        assert result == ["000001", "000002"]


class TestConfigDefaults:
    def test_market_defaults_apply_for_us(self):
        cfg = FunnelConfig.for_market("us")
        assert cfg.profile == "us"
        assert cfg.evr_min_turnover == 0.0
        assert cfg.min_avg_amount_wan == 0.0
        assert cfg.enable_accumulation_channel is False
        assert cfg.enable_dry_vol_channel is False

    def test_hk_profile_uses_hk_value_params(self):
        cfg = FunnelConfig.for_profile("hk")
        assert cfg.market_template == "hk"
        assert cfg.style_template == "hk_value"
        assert cfg.min_avg_amount_wan == 4000.0
        assert cfg.enable_ambush_channel is False
        assert cfg.spring_support_window == 70
        assert cfg.exit_stop_loss_pct == -9.0

    def test_legacy_value_aliases_resolve_to_three_profiles(self):
        assert FunnelConfig.for_profile("a_value").profile == "cn"
        assert FunnelConfig.for_profile("hk_value").profile == "hk"
        assert FunnelConfig.for_profile("us_value").profile == "us"


class TestIntegrityPolicy:
    def test_integrity_rejects_recent_missing_days(self):
        dates = pd.date_range("2024-01-01", periods=30, freq="B")
        df = _make_df(dates[:-1].strftime("%Y-%m-%d").tolist(), [10 + i * 0.1 for i in range(29)])
        ok, stats = assess_hist_integrity(
            df,
            list(dates.date),
            policy=DataIntegrityPolicy(min_coverage_200=0.9, min_coverage_20=0.9, strict_recent_days=3),
        )
        assert ok is False
        assert stats["missing_recent"] == 1


class TestL2CLiteDecision:
    def _metrics(self, **overrides) -> L2Metrics:
        base = dict(
            symbol="000001",
            market="cn",
            close=120.0,
            ma20=115.0,
            ma50=110.0,
            ma200=100.0,
            bias_200=0.20,
            rs_long=4.0,
            rs_short=2.0,
            rps_fast=86.0,
            rps_slow=82.0,
            rps_slope=0.8,
            ret_5=3.0,
            ret_10=6.0,
            ret_20=12.0,
            breakout_proximity_20=98.0,
            breakout_proximity_60=96.0,
            volume_expansion=1.4,
            price_from_250d_low=0.32,
            range_60_pct=18.0,
            dry_volume_ratio=0.55,
            ma_gap_pct=10.0,
            old_channels={
                "momentum": True,
                "ambush": False,
                "accum": False,
                "dry_vol": False,
                "rs_div": False,
                "sos": False,
            },
        )
        base.update(overrides)
        return L2Metrics(**base)

    def test_selects_one_primary_track(self):
        cfg = FunnelConfig()
        metrics = self._metrics()
        details = {
            "A": score_track_a(metrics, cfg, "cn"),
            "B": score_track_b(metrics, cfg, "cn"),
            "C": score_track_c(metrics, cfg, "cn"),
        }

        decision = select_l2_decision("000001", metrics, details, cfg, "cn")

        assert decision.passed is True
        assert decision.selected_track in {"A", "B"}
        assert isinstance(decision.track_scores, dict)

    def test_us_profile_disables_accumulation_track(self):
        cfg = FunnelConfig.for_market("us")
        metrics = self._metrics(
            market="us",
            close=40.0,
            ma20=42.0,
            ma50=45.0,
            ma200=55.0,
            bias_200=-0.27,
            rs_long=-1.0,
            rs_short=-0.5,
            rps_fast=35.0,
            rps_slow=45.0,
            rps_slope=-0.2,
            ret_20=-3.0,
            price_from_250d_low=0.12,
            ma_gap_pct=-18.0,
            old_channels={
                "momentum": False,
                "ambush": False,
                "accum": True,
                "dry_vol": True,
                "rs_div": True,
                "sos": False,
            },
        )
        details = {
            "A": score_track_a(metrics, cfg, "us"),
            "B": score_track_b(metrics, cfg, "us"),
            "C": score_track_c(metrics, cfg, "us"),
        }

        decision = select_l2_decision("AAPL", metrics, details, cfg, "us")

        assert details["C"].required_passed is False
        assert decision.selected_track != "C"

    def test_accumulation_track_rejects_severe_downtrend(self):
        cfg = FunnelConfig()
        metrics = self._metrics(
            close=80.0,
            ma20=82.0,
            ma50=90.0,
            ma200=120.0,
            ret_20=-12.0,
            price_from_250d_low=0.10,
            old_channels={
                "momentum": False,
                "ambush": False,
                "accum": True,
                "dry_vol": True,
                "rs_div": False,
                "sos": False,
            },
        )

        detail = score_track_c(metrics, cfg, "cn")

        assert detail.required_passed is False
        assert detail.reasons["severe_downtrend"] is True


class TestRunFunnelDiagnostics:
    def test_result_contains_rejection_maps(self):
        cfg = FunnelConfig()
        dates = pd.date_range("2024-01-01", periods=40, freq="B")
        weak_df = _make_df(dates.strftime("%Y-%m-%d").tolist(), [10 + i * 0.02 for i in range(40)])
        weak_df["amount"] = 1_000.0
        bench_df = _make_df(dates.strftime("%Y-%m-%d").tolist(), [100 + i * 0.1 for i in range(40)])
        bench_df["pct_chg"] = pd.Series(bench_df["close"]).pct_change() * 100.0

        result = run_funnel(
            all_symbols=["000001"],
            df_map={"000001": weak_df},
            bench_df=bench_df,
            name_map={"000001": "平安银行"},
            market_cap_map={"000001": 100.0},
            sector_map={"000001": "银行"},
            cfg=cfg,
        )

        assert result.layer1_rejections is not None
        assert result.layer1_rejections["000001"]["reason"] == "avg_amount_below_threshold"
        assert result.layer2_rejections == {}

    def test_layer2_returns_rejection_reasons_when_requested(self):
        cfg = FunnelConfig()
        dates = pd.date_range("2024-01-01", periods=60, freq="B")
        df = _make_df(dates.strftime("%Y-%m-%d").tolist(), [10 + i * 0.02 for i in range(60)])
        bench_df = _make_df(dates.strftime("%Y-%m-%d").tolist(), [100 + i * 0.1 for i in range(60)])
        bench_df["pct_chg"] = pd.Series(bench_df["close"]).pct_change() * 100.0

        passed, _channel_map, rejected = layer2_strength_detailed(
            ["000001"],
            {"000001": df},
            bench_df,
            cfg,
            return_rejections=True,
        )

        assert passed == []
        assert rejected["000001"]["reason"] == "insufficient_history"
