"""一次解析人工输入并物化版本；快照和后续策略只消费冻结值。"""

import asyncio
from datetime import date

from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.errors import ResearchIntegrityError, ResearchUnavailable
from stock_research.identity import canonical_json, digest
from stock_research.input_records import publications_for_date
from stock_research.models import Classification, ClassificationVersion, Snapshot, UniverseMember, UniverseVersion
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.input_maintenance import InputPublication, InputSources
from stock_research.store import load_snapshot


async def input_baseline(settings: Settings) -> tuple[UniverseVersion, ClassificationVersion]:
    """只读取operator固定文件，浏览器不能指定路径或替换基线。"""
    try:
        universe, classifications = await asyncio.gather(
            asyncio.to_thread(lambda: UniverseVersion.model_validate_json(settings.universe_path.read_text())),
            asyncio.to_thread(
                lambda: ClassificationVersion.model_validate_json(settings.classification_path.read_text())
            ),
        )
    except (OSError, ValueError):
        raise ResearchUnavailable("股票池或分类基线不可用，请检查operator版本文件") from None
    return universe, classifications


def manual_versions(
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    publications: tuple[InputPublication, ...],
    as_of: date,
) -> tuple[list[UniverseMember], dict[str, Classification]]:
    """只改变manual关系；新资料只补缺分类，已有行业及指数成员不由自选覆盖。"""
    members, records = list(universe.members), {row.symbol: row for row in classifications.records}
    for publication in publications:
        if publication.kind != "manual":
            continue
        definition, symbol, effective = publication.definition, publication.target, publication.effective_date
        kept = []
        for member in members:
            if member.symbol != symbol or member.index != "manual":
                kept.append(member)
            elif member.effective_date < effective:
                kept.append(
                    member.model_copy(
                        update={"expiry_date": min(member.expiry_date, effective) if member.expiry_date else effective}
                    )
                )
        members = kept
        if definition.action == "remove":
            continue
        source = f"{definition.source};manual-publication:{publication.id}:{publication.content_hash}"
        members.append(
            UniverseMember(
                symbol=symbol,
                name=definition.name,
                index="manual",
                source=source,
                effective_date=effective,
                obtained_time=publication.confirmed_time,
            )
        )
        existing = records.get(symbol)
        if existing and existing.effective_date > as_of:
            existing = None
        if existing is None or existing.industry is None:
            records[symbol] = Classification(
                symbol=symbol,
                company_name=existing.company_name if existing else definition.name,
                industry=definition.industry,
                themes=existing.themes if existing else (),
                source=source,
                obtained_time=publication.confirmed_time,
                effective_date=effective,
            )
    return members, records


def apply_input_publications(
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    publications: tuple[InputPublication, ...],
    as_of: date,
) -> tuple[UniverseVersion, ClassificationVersion]:
    """派生版本保留源摘要与发布关联，不覆盖operator文件或历史版本。"""
    if not publications:
        return universe, classifications
    members, records = manual_versions(universe, classifications, publications, as_of)
    identity = digest(
        {
            "universe": universe.model_dump(mode="json"),
            "classifications": classifications.model_dump(mode="json"),
            "publications": [row.model_dump(mode="json") for row in publications],
            "as_of": as_of.isoformat(),
        }
    )
    limitations = ("包含人工确认的当期输入，确认时间与来源已保存；不代表严格历史时点成分或分类",)
    themes = {row.target: row for row in publications if row.kind == "theme"}
    active = UniverseVersion(
        version="resolved-" + identity,
        create_time=universe.create_time,
        members=tuple(members),
        limitations=universe.limitations + limitations,
    )
    active_members = active.active(as_of)
    for publication in themes.values():
        for symbol in publication.definition.members:
            if symbol in active_members and symbol not in records:
                records[symbol] = Classification(
                    symbol=symbol,
                    company_name=active_members[symbol][0].name,
                    industry=None,
                    source=publication.definition.source,
                    obtained_time=publication.confirmed_time,
                    effective_date=publication.effective_date,
                )
    updated = []
    for symbol, record in records.items():
        if record.effective_date > as_of:
            updated.append(record)
            continue
        assigned = (set(record.themes) - themes.keys()) | {
            name for name, row in themes.items() if symbol in row.definition.members
        }
        related = [row for row in themes.values() if symbol in row.definition.members or row.target in record.themes]
        updated.append(
            record.model_copy(
                update={
                    "themes": tuple(sorted(assigned)),
                    "source": record.source
                    + "".join(f";theme-publication:{row.id}:{row.content_hash}" for row in related),
                    "obtained_time": max([record.obtained_time, *(row.confirmed_time for row in related)]),
                    "effective_date": max([record.effective_date, *(row.effective_date for row in related)]),
                }
            )
        )
    return active, ClassificationVersion(
        version="resolved-" + identity, records=tuple(updated), limitations=classifications.limitations + limitations
    )


def describe_inputs(
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    as_of: date,
    *,
    source: str,
    publications: tuple[InputPublication, ...] = (),
    baseline: tuple[UniverseVersion, ClassificationVersion] | None = None,
) -> InputSources:
    """历史来源只描述实际冻结版本，不从当前维护记录补造。"""
    return InputSources(
        source=source,
        as_of=as_of,
        universe_version=universe.version,
        classification_version=classifications.version,
        universe_hash=digest(universe.model_dump(mode="json")),
        classification_hash=digest(classifications.model_dump(mode="json")),
        base_universe_hash=digest(baseline[0].model_dump(mode="json")) if baseline else None,
        base_classification_hash=digest(baseline[1].model_dump(mode="json")) if baseline else None,
        publications=publications,
    )


async def resolve_run_inputs(store: RunStore, settings: Settings, as_of: date) -> tuple[Settings, InputSources]:
    """两类发布在一次读取中固定，物化路径由摘要生成，不暴露给HTTP请求。"""
    baseline = await input_baseline(settings)
    publications = await publications_for_date(store, as_of)
    universe, classifications = apply_input_publications(*baseline, publications, as_of)
    sources = describe_inputs(
        universe,
        classifications,
        as_of,
        source="published" if publications else "operator_baseline",
        publications=publications,
        baseline=baseline,
    )
    paths = {}
    for name, version in (("universe_path", universe), ("classification_path", classifications)):
        data = version.model_dump(mode="json")
        path = settings.artifacts_path / "inputs" / f"{name}-{digest(data)}.json"
        await asyncio.to_thread(write_once, path, canonical_json(data) + "\n")
        paths[name] = path
    return settings.model_copy(update=paths), sources


async def frozen_inputs(store: RunStore, run_id: str) -> InputSources:
    """旧运行缺来源元数据时只展示原快照，绝不取当前人工配置补历史。"""
    manifest = (await store.row(RESEARCH_RUNS, run_id))["payload"]
    snapshot = await load_snapshot(store.engine, store.workspace, manifest["snapshot_id"])
    expected = describe_inputs(snapshot.universe, snapshot.classifications, snapshot.as_of, source="frozen_snapshot")
    if not manifest.get("input_sources"):
        return expected
    return verify_input_sources(snapshot, manifest["input_sources"])


def verify_input_sources(snapshot: Snapshot, raw: dict[str, object]) -> InputSources:
    """新冻结和历史读取共用摘要绑定，物化期间改文件不能保存错误来源。"""
    try:
        sources = InputSources.model_validate(raw)
    except ValueError:
        raise ResearchIntegrityError("冻结输入来源无效") from None
    if (sources.as_of, sources.universe_hash, sources.classification_hash) != (
        snapshot.as_of,
        digest(snapshot.universe.model_dump(mode="json")),
        digest(snapshot.classifications.model_dump(mode="json")),
    ):
        raise ResearchIntegrityError("冻结输入来源与快照不一致")
    return sources
