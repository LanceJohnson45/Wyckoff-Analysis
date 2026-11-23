"""快照数据门槛、隔离、历史限制和摘要行为。"""

from datetime import date

import pytest
from pydantic import ValidationError

from stock_research.calendar import sessions_ending
from stock_research.identity import digest
from stock_research.models import Bar, ClassificationVersion, QualityIssue, Snapshot, UniverseVersion
from stock_research.quality import build_snapshot

AS_OF = date(2026, 11, 20)


def test_normal_complete_snapshot_is_reproducible(
    snapshot: Snapshot, universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]
) -> None:
    """【正常值】乱序输入仍形成同一摘要，随机 ID 不参与摘要。"""
    repeated = build_snapshot(universe, classifications, tuple(reversed(bars)), "synthetic-fixture", AS_OF)

    assert snapshot.quality.passed
    assert snapshot.quality.universe_count == 1
    assert snapshot.quality.eligible == {"martin": ("AAPL",), "wyckoff": ("AAPL",)}
    assert repeated.content_hash == snapshot.content_hash
    assert repeated.id != snapshot.id
    assert len(snapshot.quality.limitations) == 3


@pytest.mark.parametrize(
    "length,expected", [pytest.param(99, (), id="boundary-99"), pytest.param(100, ("AAPL",), id="boundary-100")]
)
def test_boundary_strategy_history(
    length: int,
    expected: tuple[str, ...],
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    bars: tuple[Bar, ...],
) -> None:
    """【边界值】100根只供 Martin，缺320根不得进入 Wyckoff。"""
    first = sessions_ending(AS_OF, length)[0]
    selected = tuple(b for b in bars if b.kind == "benchmark" or b.session_date >= first)

    result = build_snapshot(universe, classifications, selected, "fixture", AS_OF)

    assert result.quality.passed
    assert result.quality.eligible == {"martin": expected, "wyckoff": ()}


def test_error_missing_benchmark_blocks_snapshot(
    universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]
) -> None:
    """【异常值】QQQ最后一天缺失时即使股票100%覆盖也失败。"""
    selected = tuple(b for b in bars if not (b.symbol == "QQQ" and b.session_date == AS_OF))

    result = build_snapshot(universe, classifications, selected, "fixture", AS_OF)

    assert not result.quality.passed
    assert result.quality.coverage == 1
    assert any(i.symbol == "QQQ" and i.reason == "incomplete_benchmark_history" for i in result.quality.issues)


@pytest.mark.parametrize("mode", ["duplicate", "quarantine", "non_session"])
def test_error_corrupt_stock_is_quarantined(
    mode: str, universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]
) -> None:
    """【异常值】重复、云端隔离和非交易日日线阻止整股进入策略。"""
    extra = ()
    issues = ()
    if mode == "duplicate":
        extra = (bars[0],)
    elif mode == "quarantine":
        issues = (QualityIssue(symbol="AAPL", kind="stock", reason="source_quarantine:2026-11-19"),)
    else:
        extra = (bars[0].model_copy(update={"session_date": date(2026, 11, 15)}),)

    result = build_snapshot(universe, classifications, bars + extra, "fixture", AS_OF, source_issues=issues)

    assert not result.quality.passed
    assert not any(b.kind == "stock" for b in result.bars)
    assert result.quality.eligible == {"martin": (), "wyckoff": ()}


def test_error_missing_industry_excludes_only_stock(
    universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]
) -> None:
    """【异常值】行业缺失不影响日期覆盖，但股票不可进入量化池。"""
    missing = classifications.model_copy(update={"records": ()})

    result = build_snapshot(universe, missing, bars, "fixture", AS_OF)

    assert result.quality.passed
    assert result.quality.coverage == 1
    assert result.quality.eligible == {"martin": (), "wyckoff": ()}
    assert any(i.reason == "missing_industry" for i in result.quality.issues)


def test_validation_snapshot_tampering_is_rejected(snapshot: Snapshot) -> None:
    """【参数校验】物化文件的行情被修改后不能通过读取校验。"""
    payload = snapshot.model_dump(mode="json")
    payload["bars"][0]["close"] = 10.5

    with pytest.raises(ValidationError, match="摘要"):
        Snapshot.model_validate(payload)


def test_validation_future_bars_are_rejected_even_with_new_hash(snapshot: Snapshot) -> None:
    """【参数校验】重新算摘要也不能把未来日线装入历史快照。"""
    payload = snapshot.model_dump(mode="json")
    payload["bars"][0]["session_date"] = "2026-11-23"
    content = {k: v for k, v in payload.items() if k not in {"id", "create_time", "content_hash"}}
    payload["content_hash"] = digest(content)

    with pytest.raises(ValidationError, match="截止日之后"):
        Snapshot.model_validate(payload)


@pytest.mark.parametrize(
    "suspension_date,expected",
    [
        pytest.param(AS_OF, 1, id="normal-current-evidence"),
        pytest.param(date(2026, 11, 19), 0, id="error-stale-evidence"),
    ],
)
def test_boundary_suspension_is_session_scoped(
    suspension_date: date,
    expected: int,
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    bars: tuple[Bar, ...],
) -> None:
    """【边界值】只有当日停牌证据可以从覆盖分母扣除，空分母不能通过。"""
    suspended = universe.members[0].model_copy(
        update={"confirmed_suspension": "exchange-notice", "suspension_session": suspension_date}
    )
    version = universe.model_copy(update={"members": (suspended,)})

    result = build_snapshot(version, classifications, bars, "fixture", AS_OF)

    assert result.quality.suspended_count == expected
    assert result.quality.passed == (expected == 0)


@pytest.mark.parametrize(
    "missing,passed", [pytest.param(1, True, id="boundary-95pct"), pytest.param(2, False, id="boundary-90pct")]
)
def test_boundary_coverage_minimum_is_inclusive(
    missing: int, passed: bool, universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]
) -> None:
    """【边界值】20股缺1股恰好95%通过，缺2股时失败。"""
    members = tuple(universe.members[0].model_copy(update={"symbol": f"A{i}"}) for i in range(20))
    version = universe.model_copy(update={"members": members})
    stock_bar = next(b for b in bars if b.kind == "stock" and b.session_date == AS_OF)
    selected = tuple(b for b in bars if b.kind == "benchmark") + tuple(
        stock_bar.model_copy(update={"symbol": f"A{i}"}) for i in range(20 - missing)
    )

    result = build_snapshot(version, classifications, selected, "fixture", AS_OF)

    assert result.quality.passed is passed
    assert result.quality.coverage == (20 - missing) / 20
