# -*- coding: utf-8 -*-
"""tools/ 层单元测试 — 测试 Phase 2 提取的纯逻辑 Tool 函数。"""
from __future__ import annotations

from datetime import date
import pandas as pd
import pytest


# ── tools/funnel_config ──


class TestFunnelConfig:
    def test_parse_int_env_reads_env(self, monkeypatch):
        from tools.funnel_config import parse_int_env

        monkeypatch.setenv("_TEST_INT", "42")
        assert parse_int_env("_TEST_INT", 0) == 42

    def test_parse_int_env_fallback_on_missing(self, monkeypatch):
        from tools.funnel_config import parse_int_env

        monkeypatch.delenv("_TEST_INT", raising=False)
        assert parse_int_env("_TEST_INT", 7) == 7

    def test_parse_int_env_handles_float_string(self, monkeypatch):
        from tools.funnel_config import parse_int_env

        monkeypatch.setenv("_TEST_INT", "5.0")
        assert parse_int_env("_TEST_INT", 0) == 5

    def test_parse_bool_truthy(self):
        from tools.funnel_config import parse_bool

        for val in ("1", "true", "True", "yes", "on"):
            assert parse_bool(val) is True, f"Expected True for {val!r}"

    def test_parse_bool_falsy(self):
        from tools.funnel_config import parse_bool

        for val in ("0", "false", "no", "off", ""):
            assert parse_bool(val) is False, f"Expected False for {val!r}"

    def test_global_cfg_overrides_apply_only_to_cn(self, monkeypatch):
        from core.wyckoff_engine import FunnelConfig
        from tools.funnel_config import apply_funnel_cfg_overrides

        monkeypatch.setenv("FUNNEL_CFG_MIN_MARKET_CAP_YI", "35.0")
        monkeypatch.setenv("FUNNEL_CFG_MIN_AVG_AMOUNT_WAN", "5000.0")

        cn = FunnelConfig.for_market("cn")
        hk = FunnelConfig.for_market("hk")
        us = FunnelConfig.for_market("us")
        apply_funnel_cfg_overrides(cn)
        apply_funnel_cfg_overrides(hk)
        apply_funnel_cfg_overrides(us)

        assert cn.min_market_cap_yi == 35.0
        assert cn.min_avg_amount_wan == 5000.0
        assert hk.min_market_cap_yi == 0.0
        assert hk.min_avg_amount_wan == 4000.0
        assert us.min_market_cap_yi == 0.0
        assert us.min_avg_amount_wan == 0.0

    def test_market_scoped_cfg_overrides_apply_to_matching_market(self, monkeypatch):
        from core.wyckoff_engine import FunnelConfig
        from tools.funnel_config import apply_funnel_cfg_overrides

        monkeypatch.setenv("FUNNEL_CFG_US_MIN_AVG_AMOUNT_WAN", "123.0")
        monkeypatch.setenv("FUNNEL_CFG_HK_MIN_AVG_AMOUNT_WAN", "456.0")

        hk = FunnelConfig.for_market("hk")
        us = FunnelConfig.for_market("us")
        apply_funnel_cfg_overrides(hk)
        apply_funnel_cfg_overrides(us)

        assert hk.min_avg_amount_wan == 456.0
        assert us.min_avg_amount_wan == 123.0


# ── tools/report_builder ──


class TestReportBuilder:
    def test_extract_ops_codes_from_markdown_happy_path(self):
        from tools.report_builder import _extract_ops_codes_from_markdown

        report = (
            "# \u5904\u4e8e\u8d77\u8df3\u677f\n"
            "- 600056 \u4e2d\u56fd\u533b\u836f\n"
            "- 300632 \u5149\u83c6\u80a1\u4efd\n"
            "# \u903b\u8f91\u7834\u4ea7\n"
            "- 000001 \u5e73\u5b89\u94f6\u884c\n"
        )
        allowed = {"600056", "300632", "000001"}
        result = _extract_ops_codes_from_markdown(report, allowed)
        assert result == ["600056", "300632"]
        assert "000001" not in result

    def test_extract_ops_codes_empty_report(self):
        from tools.report_builder import _extract_ops_codes_from_markdown

        assert _extract_ops_codes_from_markdown("", set()) == []

    def test_try_parse_structured_report_none_on_empty(self):
        from tools.report_builder import _try_parse_structured_report

        assert _try_parse_structured_report("", set(), {}) is None

    def test_extract_json_block_strips_fences(self):
        from tools.report_builder import _extract_json_block

        raw = '```json\n{"key": "value"}\n```'
        result = _extract_json_block(raw)
        assert result == '{"key": "value"}'

    def test_extract_json_block_plain_json(self):
        from tools.report_builder import _extract_json_block

        raw = '{"a": 1}'
        assert _extract_json_block(raw) == '{"a": 1}'

    def test_extract_operation_pool_codes_happy_path(self):
        from tools.report_builder import extract_operation_pool_codes

        report = "# \u5904\u4e8e\u8d77\u8df3\u677f\n- 600056 \u4e2d\u56fd\u533b\u836f\n"
        codes = extract_operation_pool_codes(report, ["600056", "300632"])
        assert "600056" in codes

    def test_extract_operation_pool_codes_deduplicates(self):
        from tools.report_builder import extract_operation_pool_codes

        report = (
            "# \u5904\u4e8e\u8d77\u8df3\u677f\n"
            "- 600056 A\n"
            "- 600056 B\n"
        )
        codes = extract_operation_pool_codes(report, ["600056"])
        assert codes == ["600056"]


# ── tools/candidate_ranker ──


class TestCandidateRanker:
    def test_calc_close_return_pct_normal(self):
        from tools.candidate_ranker import calc_close_return_pct

        s = pd.Series([100.0, 105.0, 110.0])
        result = calc_close_return_pct(s, lookback=1)
        assert result is not None
        assert abs(result - 4.76) < 0.1  # (110-105)/105 * 100

    def test_calc_close_return_pct_short_series(self):
        from tools.candidate_ranker import calc_close_return_pct

        s = pd.Series([100.0])
        assert calc_close_return_pct(s, lookback=5) is None

    def test_calc_close_return_pct_zero_start(self):
        from tools.candidate_ranker import calc_close_return_pct

        s = pd.Series([0.0, 10.0, 20.0])
        # lookback=1 → start=10, end=20 → 100%
        result = calc_close_return_pct(s, lookback=1)
        assert result is not None
        assert abs(result - 100.0) < 0.1

    def test_trigger_labels_is_dict(self):
        from tools.candidate_ranker import TRIGGER_LABELS

        assert isinstance(TRIGGER_LABELS, dict)
        assert "sos" in TRIGGER_LABELS
        assert "spring" in TRIGGER_LABELS
        assert len(TRIGGER_LABELS) == 4


# ── tools/market_regime ──


class TestMarketRegime:
    def test_imports_callable(self):
        from tools.market_regime import analyze_benchmark_and_tune_cfg, calc_market_breadth

        assert callable(analyze_benchmark_and_tune_cfg)
        assert callable(calc_market_breadth)

    def test_calc_market_breadth_empty(self):
        from tools.market_regime import calc_market_breadth

        result = calc_market_breadth({})
        assert result["ratio_pct"] is None
        assert result["sample_size"] == 0

    def test_us_panic_repair_tunes_c_lite_thresholds(self, monkeypatch):
        from core.wyckoff_engine import FunnelConfig
        from scripts.wyckoff_funnel import _analyze_benchmark_and_tune_cfg

        monkeypatch.setenv("FUNNEL_MARKET", "us")
        cfg = FunnelConfig.for_market("us")
        dates = pd.date_range("2025-01-01", periods=220, freq="B")
        bench = pd.DataFrame(
            {
                "date": dates,
                "close": [100 + i * 0.5 for i in range(220)],
                "pct_chg": [0.0] * 217 + [0.0, 0.5, 0.5],
                "volume": [1_000_000] * 220,
            }
        )

        context = _analyze_benchmark_and_tune_cfg(bench, None, cfg)

        assert context["regime"] == "PANIC_REPAIR"
        assert cfg.min_avg_amount_wan == 0.0
        assert cfg.track_b_min_score == 58.0
        assert cfg.track_b_rps_fast_min == 55.0
        assert cfg.track_a_rs_long_min == 1.0


# ── tools/data_fetcher ──


class TestDataFetcher:
    def test_latest_trade_date_from_hist_empty(self):
        from tools.data_fetcher import latest_trade_date_from_hist

        assert latest_trade_date_from_hist(pd.DataFrame()) is None

    def test_latest_trade_date_from_hist_no_date_col(self):
        from tools.data_fetcher import latest_trade_date_from_hist

        df = pd.DataFrame({"close": [1, 2, 3]})
        assert latest_trade_date_from_hist(df) is None

    def test_latest_trade_date_from_hist_valid(self):
        from tools.data_fetcher import latest_trade_date_from_hist
        from datetime import date

        df = pd.DataFrame({"date": ["2025-01-01", "2025-01-02"]})
        result = latest_trade_date_from_hist(df)
        assert result == date(2025, 1, 2)


# ── tools/symbol_pool ──


class TestSymbolPool:
    def test_stock_name_map_callable(self):
        from tools.symbol_pool import _stock_name_map

        assert callable(_stock_name_map)


class TestFunnelPrewarm:
    def test_should_skip_prewarm_run_when_same_day_same_pool(self, tmp_path, monkeypatch):
        import scripts.funnel_prewarm as mod

        state_path = tmp_path / "prewarm_state.json"
        monkeypatch.setattr(mod, "_PREWARM_RUN_STATE_PATH", state_path)

        symbols = ["000001", "000002"]
        mod._save_prewarm_run_state(
            {
                "cn": {
                    "status": "ok",
                    "end_trade_date": "2026-07-10",
                    "trading_days": 320,
                    "symbol_count": 2,
                    "symbol_digest": mod._symbol_digest(symbols),
                    "updated_at": "2026-07-10T21:30:00",
                }
            }
        )

        should_skip, state = mod._should_skip_prewarm_run(
            market="cn",
            trading_days=320,
            end_trade_date=date(2026, 7, 10),
            symbols=symbols,
        )

        assert should_skip is True
        assert state["status"] == "ok"

    def test_symbol_manifest_record_is_ready(self, monkeypatch):
        import scripts.funnel_prewarm as mod

        monkeypatch.setattr(mod, "_RECENT_GAP_MAX_AGE_DAYS", 45)
        record = mod._ready_symbol_manifest_entry(
            end_trade_date=date(2026, 7, 10),
            trading_days=320,
            expected_count=31,
            cached_count=320,
        )

        assert mod._symbol_manifest_record_is_ready(
            record,
            end_trade_date=date(2026, 7, 10),
            trading_days=320,
        )
        assert not mod._symbol_manifest_record_is_ready(
            record,
            end_trade_date=date(2026, 7, 13),
            trading_days=320,
        )

    def test_partial_checkpoint_preserves_ready_symbols_for_resume(
        self, tmp_path, monkeypatch
    ):
        import scripts.funnel_prewarm as mod

        state_path = tmp_path / "prewarm_state.json"
        monkeypatch.setattr(mod, "_PREWARM_RUN_STATE_PATH", state_path)
        monkeypatch.setattr(mod, "_RECENT_GAP_MAX_AGE_DAYS", 45)
        symbols = ["000001", "000002"]
        end_day = date(2026, 7, 16)
        ready_record = mod._ready_symbol_manifest_entry(
            end_trade_date=end_day,
            trading_days=320,
            expected_count=31,
            cached_count=320,
        )

        manifest_ready = mod._save_prewarm_progress(
            run_state={},
            market="cn",
            status="partial",
            end_trade_date=end_day,
            trading_days=320,
            symbols=symbols,
            pool_mode="test",
            cache_ready=1,
            fast_skipped=0,
            repaired_symbols=1,
            repaired_ranges=1,
            repaired_rows=1,
            ready_updates={"000001": ready_record},
        )

        saved = mod._load_prewarm_run_state()["cn"]
        assert manifest_ready == 1
        assert saved["status"] == "partial"
        assert mod._symbol_manifest_record_is_ready(
            saved["ready_symbols"]["000001"],
            end_trade_date=end_day,
            trading_days=320,
        )

    def test_main_checkpoints_completed_symbols_during_run(
        self, tmp_path, monkeypatch
    ):
        import scripts.funnel_prewarm as mod

        state_path = tmp_path / "prewarm_state.json"
        end_day = date(2026, 7, 16)
        checkpoint_statuses = []
        real_save_progress = mod._save_prewarm_progress

        def fake_prefetch(symbol, market, trading_days, *, dry_run=False):
            return (symbol, "gap_repaired", 1, 1, 31, 320, [], 0)

        def recording_save_progress(**kwargs):
            checkpoint_statuses.append(kwargs["status"])
            return real_save_progress(**kwargs)

        monkeypatch.setattr(mod, "_PREWARM_RUN_STATE_PATH", state_path)
        monkeypatch.setattr(mod, "_PREWARM_CHECKPOINT_EVERY", 1)
        monkeypatch.setattr(mod, "_job_end_calendar_day", lambda: end_day)
        monkeypatch.setattr(
            mod,
            "_normalize_symbols",
            lambda symbols, *, market: list(symbols),
        )
        monkeypatch.setattr(mod, "_prefetch_one", fake_prefetch)
        monkeypatch.setattr(mod, "_save_prewarm_progress", recording_save_progress)
        monkeypatch.setattr(
            "sys.argv",
            [
                "funnel_prewarm.py",
                "--market",
                "cn",
                "--symbols",
                "000001,000002",
                "--trading-days",
                "320",
            ],
        )

        assert mod.main() == 0
        saved = mod._load_prewarm_run_state()["cn"]
        assert checkpoint_statuses == ["partial", "partial", "ok"]
        assert saved["status"] == "ok"
        assert set(saved["ready_symbols"]) == {"000001", "000002"}

    def test_prefetch_cache_ready_uses_single_cached_dates_lookup(self, monkeypatch):
        from types import SimpleNamespace

        import scripts.funnel_prewarm as mod

        expected_dates = [date(2026, 7, 14), date(2026, 7, 15), date(2026, 7, 16)]
        window = SimpleNamespace(
            start_trade_date=expected_dates[0],
            end_trade_date=expected_dates[-1],
        )
        calls = []

        monkeypatch.setattr(mod, "_job_end_calendar_day", lambda: expected_dates[-1])
        monkeypatch.setattr(
            mod,
            "_resolve_trading_window",
            lambda *, end_calendar_day, trading_days: window,
        )
        monkeypatch.setattr(
            mod,
            "_expected_trade_dates",
            lambda resolved_window, market: expected_dates,
        )

        def fake_load_cached_dates(*args, **kwargs):
            calls.append((args, kwargs))
            return expected_dates

        monkeypatch.setattr(mod, "load_cached_dates", fake_load_cached_dates)
        monkeypatch.setattr(
            mod,
            "fetch_stock_hist",
            lambda **kwargs: (_ for _ in ()).throw(
                AssertionError("cache-ready symbol must not fetch market data")
            ),
        )

        result = mod._prefetch_one("000001", "cn", 320)

        assert result[1] == "cache_ready"
        assert len(calls) == 1

    def test_workflows_explain_timeout_exit_124(self):
        from pathlib import Path

        root = Path(__file__).resolve().parent.parent
        for workflow_name in (
            "wyckoff_funnel.yml",
            "wyckoff_funnel_us.yml",
            "wyckoff_funnel_hk.yml",
        ):
            workflow = (root / ".github" / "workflows" / workflow_name).read_text()
            assert "prewarm_status=${PIPESTATUS[0]}" in workflow
            assert "Prewarm timeout" in workflow
            assert 'exit "${prewarm_status}"' in workflow


# ── core/strategy bridge ──


class TestStrategyBridge:
    def test_bridge_exports_are_importable(self):
        from core.strategy import run_step4

        assert callable(run_step4)


class TestDataDailyReport:
    def test_build_report_text_treats_nan_benchmark_as_na(self):
        from scripts.data_daily_report import build_report_text

        text = build_report_text(
            {
                "market": "cn",
                "end_trade_date": "2026-07-10",
                "total_symbols": 10,
                "fetch_ok": 10,
                "fetch_fail": 0,
                "integrity_pass": 10,
                "integrity_fail": 0,
                "layer1": 5,
                "layer2": 1,
                "layer3": 0,
                "total_hits": 0,
                "benchmark_context": {
                    "regime": "NEUTRAL",
                    "close": float("nan"),
                    "main_today_pct": float("nan"),
                    "breadth": {"ratio_pct": float("nan")},
                },
            }
        )

        assert "今日 N/A" in text
        assert "收盘 N/A" in text
        assert "面包量（站上MA20占比）: N/A" in text

    def test_build_report_text_includes_quality_summary(self):
        from scripts.data_daily_report import build_report_text

        text = build_report_text(
            {
                "market": "cn",
                "end_trade_date": "2026-07-10",
                "total_symbols": 10,
                "fetch_ok": 10,
                "fetch_fail": 0,
                "integrity_pass": 10,
                "integrity_fail": 0,
                "layer1": 5,
                "layer2": 1,
                "layer3": 0,
                "total_hits": 0,
                "quality_summary": {
                    "ok": 8,
                    "error_symbols": 1,
                    "warning_symbols": 1,
                    "issue_counts": {"ohlc_inconsistent": 3, "extreme_return": 1},
                    "sample_error_symbols": ["BAD"],
                    "sample_warning_symbols": ["WARN"],
                },
                "benchmark_context": {"regime": "NEUTRAL", "breadth": {"ratio_pct": 50.0}},
            }
        )

        assert "K线质量: 通过 **8**" in text
        assert "严重异常 **1**" in text
        assert "质量异常Top: ohlc_inconsistent=3、extreme_return=1" in text
        assert "质量严重样例: BAD" in text
        assert "质量警告样例: WARN" in text


class TestMainlineCnCompatibility:
    def test_run_funnel_job_populates_report_compat_metrics(self, monkeypatch):
        import scripts.wyckoff_funnel_mainline_cn as mod

        sample_df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
                "open": [10.0, 10.1, 10.2],
                "high": [10.2, 10.3, 10.4],
                "low": [9.9, 10.0, 10.1],
                "close": [10.0, 10.2, 10.3],
                "volume": [1000, 1100, 1200],
                "amount": [10_000_000, 11_000_000, 12_000_000],
                "pct_chg": [0.0, 2.0, 0.98],
            }
        )

        class _Window:
            start_trade_date = date(2024, 1, 2)
            end_trade_date = date(2024, 1, 4)

        class _Cfg:
            trading_days = 320
            require_cn_main_or_chinext = True
            min_market_cap_yi = 35.0
            amount_avg_window = 20
            l1_cap_bypass_amount_wan = 10000.0
            min_avg_amount_wan = 5000.0
            ma_short = 2
            ma_long = 3
            ma_hold = 2
            bench_drop_days = 3
            bench_drop_threshold = -2.0
            enable_rs_filter = False
            rs_window_long = 10
            rs_window_short = 3
            rs_min_long = 2.0
            rs_min_short = 1.0
            momentum_bias_200_max = 0.25
            enable_evr_trigger = True
            min_funnel_score = 0.0

        monkeypatch.setattr(mod, "FunnelConfig", lambda trading_days=320: _Cfg())
        monkeypatch.setattr(mod, "_apply_funnel_cfg_overrides", lambda cfg: None)
        monkeypatch.setattr(mod, "_resolve_trading_window", lambda **kwargs: _Window())
        monkeypatch.setattr(mod, "_resolve_funnel_end_calendar_day", lambda: date(2024, 1, 4))
        monkeypatch.setattr(
            mod,
            "_resolve_symbol_pool_from_env",
            lambda: (
                ["000001", "000002"],
                {"000001": "平安银行", "000002": "万科A"},
                {
                    "pool_mode": "test",
                    "pool_main": 2,
                    "pool_chinext": 0,
                    "pool_st_excluded": 0,
                    "pool_limit": 0,
                },
            ),
        )
        monkeypatch.setattr(mod, "fetch_sector_map", lambda: {"000001": "银行", "000002": "地产"})
        monkeypatch.setattr(mod, "fetch_market_cap_map", lambda: {"000001": 100.0, "000002": 10.0})
        monkeypatch.setattr(mod, "_stock_name_map", lambda: {"000001": "平安银行", "000002": "万科A"})
        monkeypatch.setattr(mod, "fetch_index_hist", lambda *args, **kwargs: sample_df.copy())
        monkeypatch.setattr(
            mod,
            "fetch_all_ohlcv",
            lambda **kwargs: (
                {"000001": sample_df.copy(), "000002": sample_df.copy()},
                {"fetch_ok": 2, "fetch_fail": 0, "elapsed_s": 12.0},
            ),
        )
        monkeypatch.setattr(mod, "_dump_full_fetch_snapshot", lambda **kwargs: None)
        monkeypatch.setattr(mod, "_calc_market_breadth", lambda *args, **kwargs: {"ratio_pct": 24.3, "sample_size": 2})
        monkeypatch.setattr(
            mod,
            "_analyze_benchmark_and_tune_cfg",
            lambda *args, **kwargs: {
                "regime": "NEUTRAL",
                "close": float("nan"),
                "ma50": float("nan"),
                "ma200": float("nan"),
                "ma50_slope_5d": float("nan"),
                "recent3_pct": [1.0, 2.0, 3.0],
                "recent3_cum_pct": 6.0,
                "main_today_pct": 3.0,
                "breadth": {"ratio_pct": 24.3, "sample_size": 2},
                "tuned": {},
            },
        )
        monkeypatch.setattr(mod, "layer1_filter", lambda *args, **kwargs: ["000001"])
        monkeypatch.setattr(mod, "layer2_strength_detailed", lambda *args, **kwargs: (["000001"], {"000001": "主升通道"}, []))
        monkeypatch.setattr(mod, "layer3_sector_resonance", lambda *args, **kwargs: (["000001"], ["银行"]))
        monkeypatch.setattr(mod, "analyze_sector_rotation", lambda *args, **kwargs: {"headline": "测试", "state_map": {}})
        monkeypatch.setattr(mod, "layer4_triggers", lambda *args, **kwargs: {})
        monkeypatch.setattr(mod, "detect_markup_stage", lambda *args, **kwargs: [])
        monkeypatch.setattr(mod, "detect_accum_stage", lambda *args, **kwargs: {})
        monkeypatch.setattr(mod, "layer5_exit_signals", lambda *args, **kwargs: {})
        monkeypatch.setattr(mod, "_rank_l3_candidates", lambda **kwargs: (["000001"], {"000001": 80.0}))

        _triggers, metrics = mod.run_funnel_job()

        assert metrics["market"] == "cn"
        assert metrics["end_trade_date"] == "2024-01-04"
        assert metrics["integrity_pass"] == 2
        assert metrics["integrity_fail"] == 0
        assert metrics["fetch_elapsed_s"] == 12.0
        assert metrics["layer1_rejection_top"][0]["reason"] == "market_cap_below_threshold"
        assert metrics["layer2_rejection_top"] == []
        assert metrics["benchmark_context"]["close"] is None
