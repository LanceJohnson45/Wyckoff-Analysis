"""历史截面抽样及观察窗口的固定行情边界。"""

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from core.trade_friction import round_trip_cost_pct
from stock_research.calendar import sessions_ending
from stock_research.evaluation import (
    EvaluationInput,
    candidate_sections,
    evaluate_history,
    evaluation_versions,
    section_issues,
)
from stock_research.evaluation_outcomes import OutcomeWindow, indexed_outcomes, price_index
from stock_research.evaluation_sampling import sample_sections
from stock_research.identity import SHANGHAI
from stock_research.models import Bar, ClassificationVersion, QualityIssue, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.wyckoff import WyckoffParameters

AS_OF = date(2026, 11, 20)
STAMP = datetime(2026, 11, 21, 8, tzinfo=SHANGHAI)


def outcome_windows(symbol: str, as_of: date, cutoff: date, bars: tuple[Bar, ...]) -> tuple[OutcomeWindow, ...]:
    """固定样本通过正式价格索引和观测入口计算。"""
    return indexed_outcomes(symbol, as_of, cutoff, price_index(bars))


def daily(symbol: str, day: date, close: float, *, kind: str | None = None) -> Bar:
    """只有价格路径是合成输入，不复制生产指标或信号实现。"""
    return Bar(
        symbol=symbol,
        kind=kind or ("benchmark" if symbol == "SPY" else "stock"),
        session_date=day,
        open=close,
        high=close * 1.1,
        low=close * 0.9,
        close=close,
        volume=1000,
        source="synthetic-fixture",
        source_revision=STAMP,
        observed_time=STAMP,
    )


def test_unit_outcomes_holiday_and_cost_keep_percentage_units() -> None:
    """[UNIT] 交易日窗口跨周末，净收益扣正式美股成本且超额同口径。"""
    days = sessions_ending(AS_OF, 22)
    bars = tuple(
        daily(symbol, day, start + index)
        for symbol, start in (("AAPL", 100), ("SPY", 200))
        for index, day in enumerate(days)
    )

    result = outcome_windows("AAPL", days[0], AS_OF, bars)

    first = result[0]
    assert first.status == "completed" and first.target_session == days[1]
    assert first.gross_return_pct == pytest.approx(1)
    assert first.net_return_pct == pytest.approx(1 - round_trip_cost_pct(code="AAPL.US"))
    assert first.benchmark_return_pct == pytest.approx(0.5)
    assert first.net_excess_pct == pytest.approx(0.5 - round_trip_cost_pct(code="AAPL.US"))
    assert first.mfe_pct == pytest.approx(11.1)
    assert first.mae_pct == pytest.approx(-9.1)
    assert [item.horizon for item in result] == [1, 3, 5, 10, 20]


def test_unit_outcomes_pending_ignores_even_available_future_bars() -> None:
    """[UNIT] 固定截止日之后有数据也不得提前计算标签。"""
    days = sessions_ending(AS_OF, 22)
    bars = tuple(daily(symbol, day, 100) for symbol in ("AAPL", "SPY") for day in days)

    result = outcome_windows("AAPL", days[-3], days[-2], bars)

    assert result[0].status == "completed"
    assert all(item.status == "pending" and item.net_return_pct is None for item in result[1:])


@pytest.mark.parametrize("missing", ["entry", "interior", "benchmark"], ids=["entry", "interior", "benchmark"])
def test_unit_outcomes_missing_sessions_are_not_silently_dropped(missing: str) -> None:
    """[UNIT] 不向后寻找另一根成交价，不以dropna隐藏缺口。"""
    days = sessions_ending(AS_OF, 22)
    skip = {"entry": ("AAPL", days[0]), "interior": ("AAPL", days[2]), "benchmark": ("SPY", days[2])}[missing]
    bars = tuple(daily(symbol, day, 100) for symbol in ("AAPL", "SPY") for day in days if (symbol, day) != skip)

    result = outcome_windows("AAPL", days[0], AS_OF, bars)

    assert result[1].status == "missing_data" and result[1].net_return_pct is None
    assert result[1].missing_sessions


def test_unit_outcomes_ambiguous_kind_or_duplicate_rejected() -> None:
    """[UNIT] SPY股票记录不能充当基准，相同kind日期重复拒收。"""
    days = sessions_ending(AS_OF, 22)
    bars = tuple(
        daily(symbol, day, 100, kind="benchmark" if symbol == "SPY" else "stock")
        for symbol in ("AAPL", "SPY")
        for day in days
    )
    with pytest.raises(ValueError, match="重复"):
        outcome_windows("AAPL", days[0], AS_OF, (*bars, bars[0]))

    stock_only = tuple(bar for bar in bars if bar.kind == "stock")
    result = outcome_windows("AAPL", days[0], AS_OF, stock_only)
    assert result[0].status == "missing_data"


@pytest.mark.parametrize("next_close,field", [(50.0, "mfe_pct"), (200.0, "mae_pct")], ids=["gap-down", "gap-up"])
def test_unit_outcomes_excursions_include_reference_zero(next_close: float, field: str) -> None:
    """[UNIT] 从参考收盘起算，有利/不利变化不反向取符号。"""
    days = sessions_ending(AS_OF, 2)
    bars = (
        daily("AAPL", days[0], 100),
        daily("AAPL", days[1], next_close),
        daily("SPY", days[0], 100),
        daily("SPY", days[1], 100),
    )

    result = outcome_windows("AAPL", days[0], AS_OF, bars)

    assert getattr(result[0], field) == 0


def test_unit_sampling_rank_spread_and_chronological_purge() -> None:
    """[UNIT] 按环境排序等距取20，再按时间12/4/4划分并净化跨界标签。"""
    days = sessions_ending(AS_OF, 100)
    candidates = {day: float((i * 17) % 60) for i, day in enumerate(days[:60])}

    sections = sample_sections(candidates, AS_OF)

    assert len(sections) == 20
    assert [item.as_of for item in sections] == sorted(item.as_of for item in sections)
    assert {item.spy_return_20_pct for item in sections} >= {0.0, 59.0}
    assert [item.partition for item in sections] == ["research"] * 12 + ["validation"] * 4 + ["sealed"] * 4
    assert any(item.purged for item in sections[:16])
    for item in sections:
        if item.partition == "research" and not item.purged:
            assert item.label_end < sections[12].as_of
        if item.partition == "validation" and not item.purged:
            assert item.label_end < sections[16].as_of


def test_unit_sampling_insufficient_or_unmatured_refuses_complete_claim() -> None:
    """[UNIT] 少于20个或尚未成熟的截面不伪造足量研究。"""
    days = sessions_ending(AS_OF, 40)
    with pytest.raises(ValueError, match="20"):
        sample_sections(dict.fromkeys(days[:19], 0.0), AS_OF)
    with pytest.raises(ValueError, match="观察窗口"):
        sample_sections(dict.fromkeys(days[-20:], 0.0), AS_OF)


@pytest.fixture
def evaluation_input(universe: UniverseVersion, classifications: ClassificationVersion) -> EvaluationInput:
    """360根真实日历合成OHLCV，足以抽样但不是市场研究证据。"""
    bars = tuple(
        daily(symbol, day, 100 + i * 0.02, kind="stock" if symbol == "AAPL" else "benchmark")
        for symbol in ("AAPL", "SPY", "QQQ", "IWM")
        for i, day in enumerate(sessions_ending(AS_OF, 360))
    )
    return EvaluationInput(
        cutoff=AS_OF, source_identity="synthetic-fixture", bars=bars, universe=universe, classifications=classifications
    )


def test_unit_candidates_reserve_twenty_sessions_without_peeking(evaluation_input: EvaluationInput) -> None:
    """[UNIT] 基准暖机320根后才候选，最长标签仍不得超过截止日。"""
    result = candidate_sections(evaluation_input)
    days = sessions_ending(AS_OF, 360)

    assert len(result) == 21
    assert min(result) == days[319] and max(result) == days[-21]
    assert max(result) < AS_OF


def test_unit_backfill_is_explicit_derived_and_keeps_original(evaluation_input: EvaluationInput) -> None:
    """[UNIT] 当前资料派生另起版本，原始有效日期和来源保留。"""
    original = evaluation_input.model_dump_json()
    strict = evaluation_versions(evaluation_input, allow_current=False)
    derived = evaluation_versions(evaluation_input, allow_current=True)

    assert strict == (evaluation_input.universe, evaluation_input.classifications)
    assert derived[0].version.endswith(":evaluation-current")
    assert derived[0].create_time == evaluation_input.universe.create_time
    assert derived[0].limitations[-1].startswith("当前成分")
    assert evaluation_input.model_dump_json() == original


def test_unit_quarantine_dates_cannot_exclude_past_from_future() -> None:
    """[UNIT] 未来隔离不能提前修改历史候选分母。"""
    issues = tuple(
        QualityIssue(symbol="AAPL", kind="stock", reason=f"source_quarantine:{day}")
        for day in ("2026-10-01", "2026-11-20", "2026-11-23")
    )

    result = section_issues(issues, date(2026, 11, 1), AS_OF)

    assert [issue.reason for issue in result] == ["source_quarantine:2026-11-20"]


def test_integration_evaluation_production_paths_resume_and_sealed_labels(
    evaluation_input: EvaluationInput, tmp_path: Path
) -> None:
    """[INTEGRATION] 两策略正式实现、不可变证据复用和封存标签隔离。"""
    martin, wyckoff = MartinParameters(), WyckoffParameters()
    result = evaluate_history(evaluation_input, martin, wyckoff, tmp_path)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}

    repeated = evaluate_history(evaluation_input, martin, wyckoff, tmp_path)

    assert result == repeated
    assert result.kind == "historical_quant_pool" and result.model_selection is None
    assert result.manual_review == "pending"
    assert len(result.sections) == 40
    assert {row.strategy for row in result.sections} == {"martin", "wyckoff"}
    assert all(row.universe_count == row.eligible_count == row.observation_count == 1 for row in result.sections)
    assert all(row.outcomes is None for row in result.sections if row.section.partition == "sealed")
    assert all(row.outcomes is not None for row in result.sections if row.section.partition != "sealed")
    assert result.usable_sections == {"sealed": 4}
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before


def test_integration_evaluation_failure_does_not_claim_complete_or_hide_quality(
    evaluation_input: EvaluationInput, tmp_path: Path
) -> None:
    """[INTEGRATION] 源股票无最新行情时拒绝，不能用空量化池宣称完成。"""
    damaged = evaluation_input.model_copy(
        update={"bars": tuple(bar for bar in evaluation_input.bars if bar.kind != "stock")}
    )

    with pytest.raises(ValueError, match="质量未通过"):
        evaluate_history(damaged, MartinParameters(), WyckoffParameters(), tmp_path)

    assert not list(tmp_path.iterdir())


def test_unit_evaluation_timeout_does_not_publish_success(evaluation_input: EvaluationInput, tmp_path: Path) -> None:
    """[UNIT] 总预算已经耗尽时不再开始截面计算或发布成功。"""
    with pytest.raises(TimeoutError, match="总预算"):
        evaluate_history(evaluation_input, MartinParameters(), WyckoffParameters(), tmp_path, timeout_seconds=0)
    assert not list(tmp_path.iterdir())


def test_integration_evaluation_partial_resume_and_corrupt_binding(
    evaluation_input: EvaluationInput, tmp_path: Path
) -> None:
    """[INTEGRATION] 中断保留单策略证据，坏截面清单拒绝成为新身份。"""
    from stock_research.evaluation import evaluate_section

    section = sample_sections(candidate_sections(evaluation_input), AS_OF)[0]
    versions = evaluation_versions(evaluation_input, allow_current=False)
    prices = price_index(evaluation_input.bars)
    first = evaluate_section(evaluation_input, section, versions, (MartinParameters(),), tmp_path, prices, 0.95)
    path = tmp_path / first[0].analysis_file
    before = path.read_bytes()

    resumed = evaluate_section(
        evaluation_input, section, versions, (MartinParameters(), WyckoffParameters()), tmp_path, prices, 0.95
    )

    assert len(resumed) == 2 and path.read_bytes() == before
    binding = tmp_path / f"{section.as_of}-snapshot.json"
    binding.write_text(json.dumps({}), encoding="utf-8")
    with pytest.raises(ValueError):
        evaluate_section(evaluation_input, section, versions, (MartinParameters(),), tmp_path, prices, 0.95)
    assert path.read_bytes() == before
