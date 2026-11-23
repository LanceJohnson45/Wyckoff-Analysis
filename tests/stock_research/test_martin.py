"""Martin参考因子黄金值、早期结构状态和研究参数契约。"""

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pydantic import ValidationError

from stock_research.analysis import MartinReport, compute_martin
from stock_research.factors import anchor_evidence, factor_frame
from stock_research.groups import percentiles
from stock_research.martin import early_observation, mature_observation, observe_branch, retest_episodes
from stock_research.models import Bar, Snapshot
from stock_research.parameters import MartinParameters
from stock_research.quality import build_snapshot


def test_normal_reference_factors_match_original_module_golden(trending_bars: tuple[Bar, ...]) -> None:
    """【正常值】黄金值由source@dc5fec0e实际analyze_daily生成，包含源码摘要。"""
    golden = json.loads((Path(__file__).parent / "fixtures/martin_reference.json").read_text())

    frame = factor_frame(trending_bars, MartinParameters())

    columns = {
        "ema9": "ema_fast",
        "ema21": "ema_middle",
        "ema50": "ema_slow",
        "atr14": "atr",
        "adr20": "adr",
        "return20": "return_20",
        "distance_atr": "extension",
    }
    assert len(golden["source_file_sha256"]) == 64
    for index, expected in zip(golden["indices"], golden["rows"]):
        np.testing.assert_allclose(
            [frame.iloc[index][new] for new in columns.values()],
            [expected[old] for old in columns],
            rtol=1e-12,
            atol=1e-12,
        )
    assert "强势回调" in golden["tags"]
    assert mature_observation(frame, MartinParameters()).qualified


def test_normal_factor_prefix_is_unchanged_by_future_bars(trending_bars: tuple[Bar, ...]) -> None:
    """【正常值】追加未来日线不会改变原时点EMA、ATR及突破事实。"""
    parameters = MartinParameters()

    prefix = factor_frame(trending_bars[:100], parameters)
    full = factor_frame(trending_bars, parameters)

    pd.testing.assert_frame_equal(prefix, full.iloc[:100].reset_index(drop=True))


def early_frame() -> pd.DataFrame:
    """事件固定平台100，EMA远离平台，以独立手工结构验证首回测。"""
    return pd.DataFrame(
        {
            "date": [date(2026, 11, index) for index in (16, 17, 18, 19, 20)],
            "close": [99.0, 105.0, 110.0, 108.0, 102.0],
            "low": [98.0, 104.0, 108.0, 106.0, 100.1],
            "high": [100.0, 106.0, 111.0, 109.0, 103.0],
            "atr": [2.0] * 5,
            "ema_middle": [80.0] * 5,
            "ema_fast": [85.0] * 5,
            "breakout_level": [100.0] * 5,
            "breakout": [False, True, False, False, False],
            "extension": [1.0] * 5,
            "range_ratio": [0.5] * 5,
            "volume_ratio": [0.8] * 5,
        }
    )


def test_normal_early_first_retest_does_not_require_mature_ema_order() -> None:
    """【正常值】平台首次回测可独立成立，未套入成熟分支的多头排列门槛。"""
    frame = early_frame()
    parameters = MartinParameters(slope_window=1)

    result = early_observation(frame, parameters)

    assert result.qualified
    assert result.structure["breakout_date"] == "2026-11-17"
    assert result.structure["retest_episodes"] == 1
    assert result.support["kind"] == "platform"
    assert "trend" not in result.facts


def test_error_early_invalidated_platform_is_not_revived() -> None:
    """【异常值】突破后收盘失效，即使当前重新回到支撑也排除旧结构。"""
    frame = early_frame()
    frame.loc[3, ["close", "low"]] = [98.0, 97.0]

    result = early_observation(frame, MartinParameters(slope_window=1))

    assert not result.qualified
    assert result.structure["invalidated"]
    assert "early_structure" in result.excluded_by


def test_error_second_retest_remains_distinct_from_first() -> None:
    """【异常值】离开观察带后再回到平台，记为第二次而不是新首次回测。"""
    frame = early_frame()
    frame.loc[2, "low"] = 100.0

    result = early_observation(frame, MartinParameters(slope_window=1))

    assert result.structure["retest_episodes"] == 2
    assert "first_retest" in result.excluded_by


def test_normal_first_retest_policy_can_be_disabled_without_changing_fact() -> None:
    """【正常值】研究变体可关首次门槛，但仍明确记录第二次回测事实。"""
    frame = early_frame()
    frame.loc[2, "low"] = 100.0
    original = MartinParameters(slope_window=1)
    rules = {**original.rules, "first_retest": original.rules["first_retest"].model_copy(update={"enabled": False})}

    result = early_observation(frame, original.model_copy(update={"rules": rules}))

    assert result.qualified
    assert not result.facts["first_retest"]
    assert result.structure["retest_episodes"] == 2


def test_normal_consecutive_touch_days_are_one_episode() -> None:
    """【正常值】同一观察带内连续日线只算同一次回测。"""
    frame = early_frame().iloc[2:].copy()
    frame.loc[:, "low"] = 100.0

    count = retest_episodes(frame, {"kind": "platform"}, 100.0, MartinParameters())

    assert count == 1


def test_normal_disabled_rule_retains_false_fact() -> None:
    """【正常值】禁用规则只改变排除效果，事实仍显示未满足。"""
    original = MartinParameters()
    rules = {**original.rules, "trend": original.rules["trend"].model_copy(update={"enabled": False})}
    changed = original.model_copy(update={"rules": rules})

    result = observe_branch("mature", {"trend": False, "support": True}, (), {}, changed)

    assert result.qualified
    assert result.facts["trend"] is False
    assert not result.excluded_by


def test_validation_unknown_parameters_and_rule_names_are_rejected() -> None:
    """【参数校验】拼写错误不能被静默忽略并落回默认策略。"""
    with pytest.raises(ValidationError):
        MartinParameters.model_validate({"suppport_atr": 0.75})
    with pytest.raises(ValidationError, match="规则集合"):
        MartinParameters.model_validate({"rules": {}})


def test_normal_unconfirmed_swing_is_not_an_anchor(trending_bars: tuple[Bar, ...]) -> None:
    """【正常值】右侧确认期尚未走完的极值不出现在锚点证据中。"""
    parameters = MartinParameters()
    frame = factor_frame(trending_bars, parameters)
    frame.loc[len(frame) - 2, "high"] = 1000

    anchors = anchor_evidence(frame, parameters)

    assert not any(
        anchor["kind"] == "swing_high" and anchor["date"] == str(frame.iloc[-2]["date"]) for anchor in anchors
    )
    assert all(anchor["confirmed_date"] <= str(frame.iloc[-1]["date"]) for anchor in anchors)


def test_normal_percentile_ties_and_missing_values() -> None:
    """【正常值】同值置中，缺失不以零分替代。"""
    result = percentiles({"A": 2.0, "B": 2.0, "C": None})

    assert result == {"A": 0.5, "B": 0.5}


def test_error_failed_snapshot_cannot_produce_candidate_pool(snapshot: Snapshot) -> None:
    """【异常值】整体数据门槛失败时，不把部分事实冒充可用量化池。"""
    invalid = snapshot.model_copy(update={"quality": snapshot.quality.model_copy(update={"passed": False})})

    with pytest.raises(ValueError, match="数据质量"):
        compute_martin(invalid, MartinParameters())


def test_error_zero_atr_excludes_stock_without_nonfinite_output(snapshot: Snapshot) -> None:
    """【异常值】零波动不伪造支撑距离，允许产生有明确原因的空观察池。"""
    bars = tuple(
        bar.model_copy(update={"open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0}) if bar.kind == "stock" else bar
        for bar in snapshot.bars
    )
    valid = build_snapshot(snapshot.universe, snapshot.classifications, bars, "synthetic", snapshot.as_of)

    report = compute_martin(valid, MartinParameters())

    assert report.pool == ()
    assert not report.observations
    assert any(item["reason"] == "invalid_atr_volume_or_aligned_benchmark" for item in report.excluded)
    assert MartinReport.model_validate_json(report.model_dump_json()) == report


def test_normal_report_with_missing_groups_is_finite_and_tamper_checked(
    snapshot: Snapshot, trending_bars: tuple[Bar, ...]
) -> None:
    """【正常值】单股缺组评分时重分配权重，观察产物完整摘要可校验。"""
    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + trending_bars
    valid = build_snapshot(snapshot.universe, snapshot.classifications, bars, "synthetic", snapshot.as_of)

    report = compute_martin(valid, MartinParameters())
    parsed = MartinReport.model_validate_json(report.model_dump_json())

    assert parsed.pool == ("AAPL",)
    assert parsed.model_selection is None
    assert all(np.isfinite(score) for score in parsed.observations[0].branch_scores.values())
    assert parsed.observations[0].score_components["group"] is None
    changed = parsed.model_dump(mode="json")
    changed["observations"][0]["factors"]["close"] += 1
    with pytest.raises(ValidationError, match="摘要"):
        MartinReport.model_validate(changed)
