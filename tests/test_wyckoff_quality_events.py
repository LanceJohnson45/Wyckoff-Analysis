from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from core.kline_quality import (
    check_kline_quality,
    check_kline_quality_map,
    repair_ohlc_relationship,
    summarize_quality_reports,
)
from core.signal_lifecycle import evaluate_signal_lifecycle
from core.strategy_compare import compare_strategy_runs, extract_l4_candidates
from core.wyckoff_events import classify_wyckoff_event
from scripts.compare_wyckoff_strategies import _compute_lifecycle_map, _fmt_lifecycle_brief


def test_classify_wyckoff_event_right_side_ignition():
    event = classify_wyckoff_event(
        ("sos",),
        stage="Markup",
        channel="点火破局+结构TR",
        score=12.5,
        regime="RISK_ON",
    )
    assert event.event_id == "right_side_ignition"
    assert event.label == "右侧点火"
    assert event.track == "Trend"
    assert event.confidence == "high"


def test_kline_quality_detects_bad_ohlc_and_duplicate_date():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-01", "2024-01-03"],
            "open": [10, 11, 12],
            "high": [10.5, 10.8, 13],
            "low": [9.8, 11.2, 11],
            "close": [10.2, 10.7, 12.5],
            "volume": [1000, -1, 1200],
        }
    )
    report = check_kline_quality(df, symbol="000001")
    categories = {issue.category for issue in report.issues}
    assert not report.ok
    assert "duplicate_date" in categories
    assert "ohlc_inconsistent" in categories
    assert "negative_volume" in categories


def test_kline_quality_summary_counts_symbols():
    good = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "open": [10, 10.2],
            "high": [10.5, 10.6],
            "low": [9.8, 10.0],
            "close": [10.2, 10.4],
            "volume": [1000, 1100],
        }
    )
    bad = good.assign(volume=[100, -1])
    summary = summarize_quality_reports(check_kline_quality_map({"000001": good, "000002": bad}))
    assert summary["total"] == 2
    assert summary["error_symbols"] == 1
    assert summary["ok"] == 1
    assert summary["sample_error_symbols"] == ["000002"]


def test_kline_quality_pct_change_does_not_forward_fill_missing_close():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "open": [10.0, 10.0, 30.0],
            "high": [10.5, 10.5, 31.0],
            "low": [9.8, 9.8, 29.0],
            "close": [10.0, None, 30.0],
            "volume": [1000, 1100, 1200],
        }
    )

    report = check_kline_quality(df, symbol="MISS")
    categories = {issue.category for issue in report.issues}

    assert "numeric_missing" in categories
    assert "extreme_return" not in categories


def test_kline_quality_ignores_optional_amount_missing():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "open": [10.0, 10.2],
            "high": [10.5, 10.6],
            "low": [9.8, 10.0],
            "close": [10.2, 10.4],
            "volume": [1000, 1100],
            "amount": [None, None],
        }
    )

    report = check_kline_quality(df, symbol="AMOUNT")
    categories = {issue.category for issue in report.issues}

    assert report.ok is True
    assert "numeric_missing" not in categories


def test_kline_quality_allows_large_but_plausible_daily_moves():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "open": [10.0, 10.5, 14.0],
            "high": [10.5, 14.2, 14.8],
            "low": [9.8, 10.4, 13.8],
            "close": [10.0, 14.0, 14.2],
            "volume": [1000, 1100, 1200],
        }
    )

    report = check_kline_quality(df, symbol="GAP")
    categories = {issue.category for issue in report.issues}

    assert report.ok is True
    assert "extreme_return" not in categories


def test_kline_quality_flags_implausible_price_jumps():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "open": [10.0, 19.0],
            "high": [10.5, 20.0],
            "low": [9.8, 18.8],
            "close": [10.0, 19.0],
            "volume": [1000, 1100],
        }
    )

    report = check_kline_quality(df, symbol="BADJUMP")

    assert report.ok is True
    assert any(issue.category == "extreme_return" for issue in report.issues)


def test_repair_ohlc_relationship_rebuilds_row_bounds():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "open": [10.0],
            "high": [9.9],
            "low": [10.1],
            "close": [10.2],
            "volume": [1000],
        }
    )

    repaired = repair_ohlc_relationship(df)
    report = check_kline_quality(repaired, symbol="000001")

    assert repaired.loc[0, "high"] == 10.2
    assert repaired.loc[0, "low"] == 9.9
    assert repaired.attrs["ohlc_repaired_rows"] == 1
    assert report.ok is True


def test_normalize_hist_df_repairs_ohlc_relationship():
    from core.stock_cache import normalize_hist_df

    raw = pd.DataFrame(
        {
            "日期": ["2024-01-01"],
            "开盘": [10.0],
            "最高": [9.9],
            "最低": [10.1],
            "收盘": [10.2],
            "成交量": [1000],
        }
    )

    normalized = normalize_hist_df(raw)
    report = check_kline_quality(normalized, symbol="000001")

    assert normalized.loc[0, "high"] == 10.2
    assert normalized.loc[0, "low"] == 9.9
    assert report.ok is True


def test_normalize_hist_df_fills_missing_amount_from_price_and_volume():
    from core.stock_cache import normalize_hist_df

    raw = pd.DataFrame(
        {
            "日期": ["2024-01-01", "2024-01-02"],
            "开盘": [10.0, 10.0],
            "最高": [10.5, 11.0],
            "最低": [9.8, 9.9],
            "收盘": [10.0, 11.0],
            "成交量": [1000, 1200],
        }
    )

    normalized = normalize_hist_df(raw)

    assert normalized["amount"].tolist() == [10000.0, 13200.0]


def test_normalize_hist_df_uses_existing_amount_unit_to_fill_gaps():
    from core.stock_cache import normalize_hist_df

    raw = pd.DataFrame(
        {
            "日期": ["2024-01-01", "2024-01-02"],
            "开盘": [10.0, 10.0],
            "最高": [10.5, 11.0],
            "最低": [9.8, 9.9],
            "收盘": [10.0, 11.0],
            "成交量": [1000, 1200],
            "成交额": [1000000.0, None],
        }
    )

    normalized = normalize_hist_df(raw)

    assert normalized["amount"].tolist() == [1000000.0, 1320000.0]


def test_kline_quality_keeps_unrepaired_ohlc_noise_as_error():
    df = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "open": [10.0],
            "high": [9.9],
            "low": [10.1],
            "close": [10.2],
            "volume": [1000],
        }
    )

    report = check_kline_quality(df, symbol="000001")

    assert report.ok is False
    assert any(
        issue.category == "ohlc_inconsistent" and issue.severity == "error"
        for issue in report.issues
    )


def test_signal_lifecycle_marks_done_and_pending_horizons():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2024-01-01", periods=6).astype(str),
            "close": [10, 11, 12, 11, 13, 14],
        }
    )
    lifecycle = evaluate_signal_lifecycle(df, code="000001", signal_date="2024-01-02", horizons=(1, 3, 10))
    assert lifecycle.code == "000001"
    assert lifecycle.entry_price == 11
    assert lifecycle.outcomes[0].status == "done"
    assert round(lifecycle.outcomes[0].return_pct, 2) == 9.09
    assert lifecycle.outcomes[-1].status == "pending"


def test_signal_lifecycle_uses_future_low_for_drawdown():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2024-01-01", periods=3).astype(str),
            "close": [10, 11, 12],
            "low": [7, 8.5, 10.5],
        }
    )
    lifecycle = evaluate_signal_lifecycle(df, code="000001", signal_date="2024-01-01", horizons=(1,))
    assert round(lifecycle.outcomes[0].return_pct, 2) == 10.0
    assert round(lifecycle.outcomes[0].max_drawdown_pct, 2) == -15.0


def test_strategy_compare_extracts_and_compares_candidates():
    result_a = SimpleNamespace(
        triggers={"sos": [("000001", 5.0)], "spring": [("000002", 4.0)]},
        stage_map={"000001": "Markup"},
        channel_map={"000001": "主升通道"},
    )
    result_b = SimpleNamespace(
        triggers={"sos": [("000001", 3.0)], "lps": [("000003", 2.0)]},
        stage_map={},
        channel_map={},
    )
    run_a = SimpleNamespace(strategy_id="a", candidates=extract_l4_candidates(result_a, "a"))
    run_b = SimpleNamespace(strategy_id="b", candidates=extract_l4_candidates(result_b, "b"))
    comparison = compare_strategy_runs((run_a, run_b))
    assert comparison.counts["intersection"] == 1
    assert "000001" in comparison.intersection
    assert comparison.counts["only_a"] == 1
    assert comparison.counts["only_b"] == 1


def test_compare_script_lifecycle_map_and_brief():
    df = pd.DataFrame(
        {
            "date": pd.bdate_range("2024-01-01", periods=6).astype(str),
            "close": [10, 11, 12, 11, 13, 14],
            "low": [9.5, 10.5, 11.5, 10.8, 12.2, 13.2],
        }
    )
    run = SimpleNamespace(
        candidates=(SimpleNamespace(code="000001", triggers=("sos",), score=5.0, stage="Markup", channel="主升通道"),)
    )
    metrics = {
        "_debug": {
            "all_df_map": {"000001": df},
            "end_trade_date": "2024-01-02",
        }
    }
    lifecycle_map = _compute_lifecycle_map(run=run, metrics=metrics, horizons=(1, 3))
    assert "000001" in lifecycle_map
    assert lifecycle_map["000001"]["done_count"] >= 1
    brief = _fmt_lifecycle_brief(lifecycle_map["000001"])
    assert "lifecycle H1=" in brief
