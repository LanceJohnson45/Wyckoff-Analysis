"""按运行冻结的量化事实读取，不启动计算、模型或行情网络请求。"""

from dataclasses import dataclass
from typing import Literal

from stock_research.analysis import MartinReport
from stock_research.errors import ResearchIntegrityError, ResearchNotFound
from stock_research.evidence import flatten_facts
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.queries import company_views, run_view
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS, RESEARCH_REPORTS
from stock_research.schemas.api import RunView, Strategy
from stock_research.schemas.observations import GroupObservation, HistoryBar, StockObservation, StrategyObservation
from stock_research.store import load_snapshot
from stock_research.wyckoff import WyckoffReport


async def frozen_analyses(
    store: RunStore, manifest: dict[str, object], snapshot: Snapshot
) -> dict[str, tuple[str, MartinReport | WyckoffReport]]:
    """逐份校验运行绑定，不能把另一份合法产物拼入本运行。"""
    analyses = {}
    for strategy, model in (("martin", MartinReport), ("wyckoff", WyckoffReport)):
        identifier = manifest["factor_results"][strategy]
        row = await store.row(FACTOR_RESULTS, identifier)
        try:
            artifact = model.model_validate(row["payload"])
        except ValueError:
            raise ResearchIntegrityError("冻结量化产物无效") from None
        if (
            row["strategy"] != strategy
            or row["snapshot_uuid"] != snapshot.id
            or artifact.snapshot_id != snapshot.id
            or artifact.snapshot_hash != snapshot.content_hash
            or manifest["snapshot_hash"] != snapshot.content_hash
            or artifact.content_hash != manifest["analysis_hashes"][strategy]
        ):
            raise ResearchIntegrityError("冻结量化产物与运行不一致")
        analyses[strategy] = identifier, artifact
    return analyses


def strategy_observation(
    strategy: Strategy, identifier: str, analysis: MartinReport | WyckoffReport, symbol: str
) -> StrategyObservation:
    """引用保持原始路径；排除与缺计算不伪装成零因子或模型否定。"""
    facts, exclusions = {}, {}
    available = False
    for observation in analysis.observations:
        row = observation.model_dump(mode="json") if isinstance(analysis, MartinReport) else observation
        if row["symbol"] == symbol:
            available = True
            flatten_facts(row, f"{strategy}/{symbol}", symbol, facts)
    if isinstance(analysis, WyckoffReport):
        for row in analysis.candidates:
            if row["symbol"] == symbol:
                flatten_facts(row, f"wyckoff/{symbol}/candidate", symbol, facts)
    for index, row in enumerate(analysis.excluded):
        if row.get("symbol") == symbol:
            flatten_facts(row, f"{strategy}/{symbol}/excluded/{index}", symbol, exclusions)
    return StrategyObservation(
        strategy=strategy,
        artifact_id=identifier,
        content_hash=analysis.content_hash,
        parameter_hash=analysis.parameter_hash,
        parameters=analysis.parameters,
        factor_definitions=analysis.factor_definitions if isinstance(analysis, MartinReport) else {},
        effective_config=analysis.effective_config if isinstance(analysis, WyckoffReport) else None,
        in_pool=symbol in analysis.pool,
        calculation_available=available,
        facts=tuple(facts.values()),
        exclusions=tuple(exclusions.values()),
        limitations=analysis.limitations,
    )


async def stock_observation(
    store: RunStore, run_id: str, symbol: str, limit: int, report_id: str | None = None
) -> StockObservation:
    """股票身份限于该运行的股票池，历史始终截断到冻结截止日。"""
    return stock_from_scope(await frozen_scope(store, run_id, report_id), symbol, limit)


@dataclass(frozen=True)
class FrozenResearch:
    """多股比较共享一次已验证的历史修订和量化产物。"""

    report_id: str | None
    manifest: dict[str, object]
    snapshot: Snapshot
    analyses: dict[str, tuple[str, MartinReport | WyckoffReport]]
    view: RunView


async def frozen_scope(store: RunStore, run_id: str | None, report_id: str | None) -> FrozenResearch:
    """固定修订不读取后来意见；兼容原股票查询的当前run视图。"""
    body = (await store.row(RESEARCH_REPORTS, report_id))["payload"] if report_id else await store.summary(run_id)
    if run_id is not None and body["run_id"] != run_id:
        raise ResearchNotFound("报告不属于指定运行")
    manifest = body["manifest"]
    snapshot = await load_snapshot(store.engine, store.workspace, manifest["snapshot_id"])
    analyses = await frozen_analyses(store, manifest, snapshot)
    return FrozenResearch(report_id, manifest, snapshot, analyses, await run_view(store, body, snapshot=snapshot))


def stock_from_scope(scope: FrozenResearch, symbol: str, limit: int) -> StockObservation:
    """同一上下文内投影原始股票事实，不再为每只股票重读报告。"""
    snapshot, analyses, view = scope.snapshot, scope.analyses, scope.view
    if symbol not in snapshot.universe.active(snapshot.as_of):
        raise ResearchNotFound("证券不在本次冻结股票池")
    history, history_total = frozen_history(snapshot, "stock", symbol, limit)
    return StockObservation(
        run_id=view.run_id,
        report_id=scope.report_id,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        as_of=snapshot.as_of,
        company=company_views(snapshot, {symbol})[0],
        observations=tuple(
            strategy_observation(key, identifier, item, symbol) for key, (identifier, item) in analyses.items()
        ),
        history=history,
        history_total=history_total,
        groups=tuple(
            GroupObservation(**group, ref_prefix="group/" + digest(group["name"])[:16])
            for group in analyses["martin"][1].groups
            if symbol in group["members"]
        ),
        selections=tuple(item for item in view.selections if symbol in {row.symbol for row in item.decision.items}),
        reviews=tuple(item for item in view.reviews if symbol in {row.symbol for row in item.decision.items}),
        limitations=snapshot.quality.limitations,
    )


def frozen_history(
    snapshot: Snapshot, kind: Literal["stock", "benchmark"], symbol: str, limit: int
) -> tuple[tuple[HistoryBar, ...], int]:
    """只投影该冻结快照内最后N根日线，引用路径沿原证据格式。"""
    bars = [bar for bar in snapshot.bars if bar.kind == kind and bar.symbol == symbol]
    return tuple(
        HistoryBar(
            **bar.model_dump(include={"session_date", "open", "high", "low", "close", "volume"}),
            ref_prefix=f"bar/{kind}/{symbol}/{bar.session_date}",
        )
        for bar in bars[-limit:]
    ), len(bars)
