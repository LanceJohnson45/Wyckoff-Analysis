"""按运行冻结的量化事实读取，不启动计算、模型或行情网络请求。"""

from stock_research.analysis import MartinReport
from stock_research.errors import ResearchIntegrityError, ResearchNotFound
from stock_research.evidence import flatten_facts
from stock_research.identity import digest
from stock_research.models import Snapshot
from stock_research.queries import company_views, run_view
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS
from stock_research.schemas.api import Strategy
from stock_research.schemas.observations import GroupObservation, StockObservation, StrategyObservation
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
        in_pool=symbol in analysis.pool,
        calculation_available=available,
        facts=tuple(facts.values()),
        exclusions=tuple(exclusions.values()),
        limitations=analysis.limitations,
    )


async def stock_observation(store: RunStore, run_id: str, symbol: str, limit: int) -> StockObservation:
    """股票身份限于该运行的股票池，历史始终截断到冻结截止日。"""
    body = await store.summary(run_id)
    manifest = body["manifest"]
    snapshot = await load_snapshot(store.engine, store.workspace, manifest["snapshot_id"])
    if symbol not in snapshot.universe.active(snapshot.as_of):
        raise ResearchNotFound("证券不在本次冻结股票池")
    analyses = await frozen_analyses(store, manifest, snapshot)
    view = await run_view(store, body, snapshot=snapshot)
    history = [bar for bar in snapshot.bars if bar.kind == "stock" and bar.symbol == symbol]
    return StockObservation(
        run_id=run_id,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        as_of=snapshot.as_of,
        company=company_views(snapshot, {symbol})[0],
        observations=tuple(
            strategy_observation(key, identifier, item, symbol) for key, (identifier, item) in analyses.items()
        ),
        history=tuple(
            dict(
                **bar.model_dump(include={"session_date", "open", "high", "low", "close", "volume"}),
                ref_prefix=f"bar/stock/{symbol}/{bar.session_date}",
            )
            for bar in history[-limit:]
        ),
        history_total=len(history),
        groups=tuple(
            GroupObservation(**group, ref_prefix="group/" + digest(group["name"])[:16])
            for group in analyses["martin"][1].groups
            if symbol in group["members"]
        ),
        selections=tuple(item for item in view.selections if symbol in {row.symbol for row in item.decision.items}),
        reviews=tuple(item for item in view.reviews if symbol in {row.symbol for row in item.decision.items}),
        limitations=snapshot.quality.limitations,
    )
