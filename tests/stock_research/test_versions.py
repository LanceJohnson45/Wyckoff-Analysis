"""成分并集和分类缓存的有出处版本构建。"""

import json
from datetime import date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from stock_research.identity import SHANGHAI
from stock_research.models import UniverseVersion
from stock_research.versions import build_classifications, build_universe, import_version

AS_OF = date(2026, 11, 20)


def test_normal_build_union_preserves_membership_and_source(tmp_path: Path) -> None:
    """【正常值】两份独立清单构建550股并集，交集仍保留两个指数来源。"""
    sp500, nasdaq = tmp_path / "sp500.json", tmp_path / "nasdaq.json"
    sp500.write_text(json.dumps([{"symbol": f"A{i}", "name": f"Company {i}"} for i in range(500)]))
    nasdaq.write_text(json.dumps([{"symbol": f"A{i}", "name": f"Company {i}"} for i in range(450, 550)]))

    universe = build_universe(sp500, nasdaq, version="v1", effective_date=AS_OF, source_reference="synthetic")
    output = tmp_path / "universe.json"
    raw = tmp_path / "input.json"
    raw.write_text(universe.model_dump_json())
    import_version("universe", raw, output)

    assert len(universe.active(AS_OF)) == 550
    assert {m.index for m in universe.active(AS_OF)["A450"]} == {"sp500", "nasdaq100"}
    assert all("sha256:" in m.source for m in universe.members)
    assert UniverseVersion.model_validate_json(output.read_text()) == universe


def test_error_partial_index_cannot_be_imported(tmp_path: Path, universe: UniverseVersion) -> None:
    """【异常值】不完整指数清单不能伪装完整研究股票池。"""
    raw, destination = tmp_path / "input.json", tmp_path / "universe.json"
    raw.write_text(universe.model_dump_json())

    with pytest.raises(ValueError, match="成分数量"):
        import_version("universe", raw, destination)

    assert not destination.exists()


def test_normal_classification_keeps_original_obtained_time(tmp_path: Path, universe: UniverseVersion) -> None:
    """【正常值】旧缓存时间不被导入时间覆盖，主题保持未分配。"""
    cache = tmp_path / "cache.json"
    cache.write_text(
        json.dumps(
            {
                "updated_at": "2026-04-22T08:00:00+08:00",
                "items": {
                    "AAPL": {"market": "us", "industry": "Hardware", "updated_at": "2026-04-20T08:00:00+08:00"},
                },
            }
        )
    )

    result = build_classifications(cache, universe, version="v1", effective_date=AS_OF, source_reference="synthetic")

    assert result.records[0].obtained_time == datetime(2026, 4, 20, 8, tzinfo=SHANGHAI)
    assert result.records[0].themes == ()
    assert result.limitations


def test_validation_overlapping_membership_rejected(universe: UniverseVersion) -> None:
    """【参数校验】同股同指数的重叠有效期必须拒绝。"""
    member = universe.members[0].model_copy(update={"effective_date": date(2025, 1, 1)})

    with pytest.raises(ValidationError, match="重叠"):
        UniverseVersion(version="overlap", members=(*universe.members, member))
