"""固定公开源提交与响应，测试不向外网请求。"""

import base64
import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError

from stock_research.identity import now
from stock_research.models import Snapshot
from stock_research.public_versions import (
    ConstituentSource,
    build_public_versions,
    fetch_constituents,
    parse_constituents,
)


def source(index: str, body: str) -> ConstituentSource:
    """构造有全文摘要和固定提交的合成公开源。"""
    return ConstituentSource(
        index=index,
        body=body,
        repository="synthetic/repository",
        path="data/file",
        commit="a" * 40,
        revision_time=now(),
        obtained_time=now(),
        content_sha256=hashlib.sha256(body.encode()).hexdigest(),
    )


@pytest.fixture
def sources() -> tuple[ConstituentSource, ...]:
    """两指数有100股交集，分类只取有明确语义的GICS列。"""
    sp = "Symbol,Security,GICS Sub-Industry\n" + "\n".join(f"A{i},Company {i},Hardware" for i in range(500))
    ndx = json.dumps(
        [
            {"Ticker": f"A{i}", "Company": f"Company {i}", "GICS_Sub_Industry": "ambiguous-ignore"}
            for i in range(400, 500)
        ]
    )
    return source("sp500", sp), source("nasdaq100", ndx)


def test_normal_public_versions_keep_overlap_and_provenance(sources: tuple[ConstituentSource, ...]) -> None:
    """【正常值】多重归属、来源提交和研究适用日都保留。"""
    universe, groups = build_public_versions(sources, "synthetic-v1", date(2026, 11, 20))

    assert len(universe.members) == 600
    assert len(universe.active(date(2026, 11, 20))) == 500
    assert {m.index for m in universe.active(date(2026, 11, 20))["A450"]} == {"sp500", "nasdaq100"}
    assert all("@" + "a" * 40 in member.source for member in universe.members)
    assert {group.industry for group in groups.records} == {"GICS:Hardware"}
    assert universe.limitations


def test_error_public_source_hash_mismatch_is_rejected(sources: tuple[ConstituentSource, ...]) -> None:
    """【异常值】来源全文被修改后不能形成新版本。"""
    changed = sources[0].model_copy(update={"body": sources[0].body.replace("Hardware", "Other")})

    with pytest.raises(ValueError, match="摘要"):
        parse_constituents(changed)


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("Ticker,Name\nAAPL,Apple", id="error-csv-schema"),
        pytest.param("<html>upstream error</html>", id="error-html-response"),
    ],
)
def test_error_changed_sp500_schema_is_rejected(body: str) -> None:
    """【异常值】字段变更或错误页不能伪装成成分清单。"""
    with pytest.raises(ValueError):
        parse_constituents(source("sp500", body))


def test_validation_normalized_duplicate_member_is_rejected(sources: tuple[ConstituentSource, ...]) -> None:
    """【参数校验】点号和连字符同股不能在同指数重复。"""
    changed = source("sp500", sources[0].body + "\nBRK.B,Berkshire,Insurance\nBRK-B,Berkshire,Insurance")

    with pytest.raises(ValidationError, match="重复成员"):
        build_public_versions((changed, sources[1]), "duplicate", date(2026, 11, 20))


@pytest.mark.parametrize(
    "age,expected",
    [
        pytest.param(1, True, id="normal-fresh"),
        pytest.param(40, False, id="error-stale"),
        pytest.param(-1, False, id="error-future"),
    ],
)
def test_boundary_source_revision_age_is_enforced(age: int, expected: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    """【边界值】请求固定提交，不接受过期或未来的版本时间。"""
    body = '[{"Ticker":"AAPL","Company":"Apple"}]'
    commit = "b" * 40
    stamp = (now() - timedelta(days=age)).isoformat()

    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/commits"):
            return httpx.Response(200, json=[{"sha": commit, "commit": {"committer": {"date": stamp}}}])
        assert request.url.params["ref"] == commit
        return httpx.Response(
            200, json={"encoding": "base64", "size": len(body), "content": base64.b64encode(body.encode()).decode()}
        )

    client_type = httpx.Client
    monkeypatch.setattr(
        "stock_research.public_versions.httpx.Client",
        lambda **kwargs: client_type(**kwargs, transport=httpx.MockTransport(respond)),
    )

    if expected:
        result = fetch_constituents("nasdaq100", "synthetic/repository", "data/file.json", 35)
        assert result.commit == commit
        assert result.body == body
    else:
        with pytest.raises(ValueError, match="更新时间"):
            fetch_constituents("nasdaq100", "synthetic/repository", "data/file.json", 35)


def test_error_refresh_failure_does_not_publish_completion(
    tmp_path: Path, sources: tuple[ConstituentSource, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    """【异常值】第二指数取数失败时不发布完成标志，也不更改既有文件。"""
    from stock_research.config import Settings
    from stock_research.public_versions import refresh_versions

    existing = tmp_path / "universe.json"
    existing.write_text("original")

    def fetch(index: str, *args: object) -> ConstituentSource:
        if index == "sp500":
            return sources[0]
        raise ValueError("synthetic upstream failure")

    monkeypatch.setattr("stock_research.public_versions.fetch_constituents", fetch)

    with pytest.raises(ValueError, match="upstream failure"):
        refresh_versions(Settings(_env_file=None), "v1", date(2026, 11, 20), tmp_path)

    assert existing.read_text() == "original"
    assert not (tmp_path / "complete.json").exists()


def test_normal_refresh_retains_existing_manual_members_and_themes(
    tmp_path: Path, sources: tuple[ConstituentSource, ...]
) -> None:
    """【正常值】刷新指数时已生效的人工自选、分类与主题不丢失。"""
    from stock_research.config import Settings
    from stock_research.models import Classification, ClassificationVersion, UniverseMember, UniverseVersion
    from stock_research.public_versions import preserve_manual_versions

    universe, groups = build_public_versions(sources, "next", date(2026, 11, 20))
    manual = UniverseMember(
        symbol="CUSTOM",
        name="Custom",
        index="manual",
        source="human",
        effective_date=date(2026, 1, 1),
        obtained_time=now(),
    )
    previous = UniverseVersion(version="previous", members=(manual,))
    records = tuple(
        Classification(
            symbol=symbol,
            company_name=symbol,
            industry="approved:Hardware",
            themes=("Human theme",),
            source="human-confirmed",
            obtained_time=now(),
            effective_date=date(2026, 1, 1),
        )
        for symbol in ("A0", "CUSTOM")
    )
    upath, cpath = tmp_path / "universe.json", tmp_path / "classifications.json"
    upath.write_text(previous.model_dump_json())
    cpath.write_text(ClassificationVersion(version="previous", records=records).model_dump_json())
    settings = Settings(_env_file=None, universe_path=upath, classification_path=cpath)

    result, classifications = preserve_manual_versions(settings, universe, groups, date(2026, 11, 20))
    by_symbol = {record.symbol: record for record in classifications.records}

    assert result.active(date(2026, 11, 20))["CUSTOM"] == (manual,)
    assert by_symbol["A0"].industry == "GICS:Hardware"
    assert by_symbol["A0"].themes == ("Human theme",)
    assert "human-confirmed" in by_symbol["A0"].source
    assert by_symbol["CUSTOM"].industry == "approved:Hardware"


@pytest.mark.parametrize(
    "industry,offset,retained",
    [
        pytest.param("GICS:Semiconductors", 0, True, id="normal-supplement"),
        pytest.param("GICS:Semiconductors", 1, False, id="error-future"),
        pytest.param("ICB:Technology", 0, False, id="error-other-taxonomy"),
    ],
)
def test_normal_refresh_preserves_effective_gics_supplements(
    tmp_path: Path, snapshot: Snapshot, industry: str, offset: int, retained: bool
) -> None:
    """【正常/异常值】缺分类成员沿用有出处的同口径补充，未来和异口径资料不混入。"""
    from stock_research.config import Settings
    from stock_research.models import ClassificationVersion
    from stock_research.public_versions import preserve_manual_versions

    previous = ClassificationVersion(
        version="supplement",
        records=(
            snapshot.classifications.records[0].model_copy(
                update={"industry": industry, "effective_date": snapshot.as_of + timedelta(days=offset)}
            ),
        ),
        limitations=("supplement-source-is-not-refreshed",),
    )
    path = tmp_path / "classifications.json"
    path.write_text(previous.model_dump_json())
    settings = Settings(_env_file=None, universe_path=tmp_path / "missing", classification_path=path)

    _, result = preserve_manual_versions(
        settings, snapshot.universe, ClassificationVersion(version="new", records=()), snapshot.as_of
    )

    assert len(result.records) == int(retained)
    if retained:
        assert result.records == previous.records
        assert previous.limitations[0] in result.limitations
