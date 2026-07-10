# -*- coding: utf-8 -*-
"""core/funnel_pipeline.py re-export 桥接测试。"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

akshare = pytest.importorskip("akshare", reason="akshare not installed")


def test_bridge_exports_are_importable():
    """确认桥接模块能正常 import 所有公共 API。"""
    from core.funnel_pipeline import (
        TRIGGER_LABELS,
        analyze_benchmark_and_tune_cfg,
        calc_market_breadth,
        rank_l3_candidates,
        run_funnel,
        run_funnel_job,
    )
    assert isinstance(TRIGGER_LABELS, (dict, list, tuple))
    assert callable(run_funnel)
    assert callable(run_funnel_job)
    assert callable(analyze_benchmark_and_tune_cfg)
    assert callable(calc_market_breadth)
    assert callable(rank_l3_candidates)


def test_cn_funnel_job_no_longer_runs_post_300day_engine(monkeypatch):
    import scripts.wyckoff_funnel as funnel

    monkeypatch.setattr(funnel, "_should_delegate_cn_to_mainline", lambda: False)

    sample_df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "open": [10.0, 10.2],
            "high": [10.3, 10.4],
            "low": [9.9, 10.1],
            "close": [10.2, 10.3],
            "volume": [1_000_000, 1_100_000],
        }
    )

    class _Window:
        start_trade_date = date(2024, 1, 2)
        end_trade_date = date(2024, 1, 3)

    monkeypatch.setattr(funnel, "EXECUTOR_MODE", "thread")
    monkeypatch.setattr(funnel, "BATCH_SIZE", 10)
    monkeypatch.setattr(funnel, "BATCH_SLEEP", 0.0)
    monkeypatch.setattr(funnel, "MAX_WORKERS", 1)
    monkeypatch.setattr(funnel, "BATCH_TIMEOUT", 5)

    monkeypatch.setattr(funnel, "_resolve_funnel_market", lambda: "cn")
    monkeypatch.setattr(funnel, "_job_end_calendar_day", lambda: date(2024, 1, 3))
    monkeypatch.setattr(funnel, "_resolve_trading_window", lambda **kwargs: _Window())
    monkeypatch.setattr(
        funnel,
        "_resolve_symbol_pool_from_env",
        lambda: (
            ["000001"],
            {"000001": "平安银行"},
            {
                "pool_mode": "test",
                "pool_main": 1,
                "pool_chinext": 0,
                "pool_st_excluded": 0,
                "pool_limit": 1,
            },
        ),
    )
    monkeypatch.setattr(funnel, "fetch_sector_map", lambda: {"000001": "银行"})
    monkeypatch.setattr(funnel, "fetch_industry_map", lambda: {"000001": "银行"})
    monkeypatch.setattr(funnel, "fetch_market_cap_map", lambda market="cn": {"000001": 1e11})
    monkeypatch.setattr(funnel, "_stock_name_map", lambda market="cn": {"000001": "平安银行"})
    monkeypatch.setattr(funnel, "fetch_index_hist", lambda *args, **kwargs: sample_df.copy())
    monkeypatch.setattr(
        funnel,
        "_fetch_one_with_retry_thread",
        lambda sym, window: (sym, sample_df.copy()),
    )
    monkeypatch.setattr(
        funnel,
        "_expected_trade_dates",
        lambda *args, **kwargs: ([date(2024, 1, 2), date(2024, 1, 3)], "test"),
    )
    monkeypatch.setattr(
        funnel,
        "filter_symbols_by_integrity",
        lambda df_map, expected_dates, policy: (df_map, {}),
    )
    monkeypatch.setattr(
        funnel,
        "build_market_cap_map_from_shares",
        lambda **kwargs: (kwargs["base_map"], {"computed": 0, "refreshed": 0, "missing_shares": 0, "missing_close": 0, "total": len(kwargs["symbols"])}),
    )
    monkeypatch.setattr(funnel, "_dump_full_fetch_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(funnel, "_calc_market_breadth", lambda *args, **kwargs: {"pct_above_ma": 50.0})
    monkeypatch.setattr(
        funnel,
        "_analyze_benchmark_and_tune_cfg",
        lambda *args, **kwargs: {
            "regime": "NEUTRAL",
            "close": 10.0,
            "ma50": 9.5,
            "ma200": 9.0,
            "ma50_slope_5d": 0.1,
            "recent3_pct": 0.5,
            "recent3_cum_pct": 1.0,
            "main_today_pct": 0.2,
            "smallcap_code": "399006",
            "smallcap_today_pct": 0.3,
            "breadth": {"pct_above_ma": 50.0},
            "panic_triggered": False,
            "panic_reasons": [],
            "repair_triggered": False,
            "repair_reasons": [],
            "tuned": False,
        },
    )
    monkeypatch.setattr(
        funnel,
        "layer1_filter",
        lambda *args, **kwargs: (["000001"], {}) if kwargs.get("return_rejections") else ["000001"],
    )
    monkeypatch.setattr(
        funnel,
        "layer2_strength_detailed",
        lambda *args, **kwargs: (["000001"], {"000001": "主升确认"}, {}) if kwargs.get("return_rejections") else (["000001"], {"000001": "主升确认"}),
    )
    monkeypatch.setattr(funnel, "layer3_sector_resonance", lambda *args, **kwargs: (["000001"], ["银行"]))
    monkeypatch.setattr(
        funnel,
        "analyze_sector_rotation",
        lambda *args, **kwargs: {"headline": "测试", "state_map": {}},
    )
    monkeypatch.setattr(funnel, "layer4_triggers", lambda *args, **kwargs: {"spring": [("000001", 88.0)]})
    monkeypatch.setattr(funnel, "detect_markup_stage", lambda *args, **kwargs: [])
    monkeypatch.setattr(funnel, "detect_accum_stage", lambda *args, **kwargs: {"000001": "A"})
    monkeypatch.setattr(funnel, "layer5_exit_signals", lambda *args, **kwargs: {})
    monkeypatch.setattr(funnel, "_rank_l3_candidates", lambda **kwargs: (["000001"], {"000001": 88.0}))

    _triggers, metrics = funnel.run_funnel_job()

    assert metrics["market"] == "cn"
    assert "three_hundred_day" not in metrics


def test_analyze_benchmark_treats_trailing_nan_as_missing_benchmark():
    import scripts.wyckoff_funnel as funnel
    from core.wyckoff_engine import FunnelConfig

    dates = pd.date_range("2024-01-01", periods=220, freq="B")
    closes = [3000.0 + i for i in range(219)] + [float("nan")]
    pct = [0.1] * 220
    pct[-1] = -1.2
    volume = [1_000_000] * 220
    bench_df = pd.DataFrame(
        {
            "date": dates,
            "close": closes,
            "pct_chg": pct,
            "volume": volume,
        }
    )

    context = funnel._analyze_benchmark_and_tune_cfg(
        bench_df,
        None,
        FunnelConfig(),
        breadth={"ratio_pct": 20.0, "prev_ratio_pct": 35.0, "delta_pct": -15.0, "sample_size": 100},
    )

    assert context["close"] is None
    assert context["ma50"] is None
    assert context["ma200"] is None
    assert context["ma50_slope_5d"] is None
    assert context["has_main_benchmark"] is False
    assert context["regime"] == "RISK_OFF"
