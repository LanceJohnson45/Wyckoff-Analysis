"""从冻结来源投影完整有效股票池，由PG完成字面搜索和稳定分页。"""

from collections import defaultdict

from sqlalchemy import JSON, bindparam, cast, column, func, or_, select

from stock_research.analysis import MartinReport
from stock_research.observations import FrozenResearch, frozen_scope
from stock_research.queries import company_views
from stock_research.report_queries import report_scope
from stock_research.run_store import RunStore
from stock_research.schemas.universe import UniversePage, UniverseQuery, UniverseStock, UniverseStrategy


def universe_rows(scope: FrozenResearch) -> tuple[UniverseStock, ...]:
    """不按行情或分类内连接删成员，状态沿原产物与本修订已提交意见。"""
    snapshot = scope.snapshot
    members = snapshot.universe.active(snapshot.as_of)
    classified = {row.symbol: row for row in snapshot.classifications.records if row.effective_date <= snapshot.as_of}
    histories = defaultdict(list)
    for bar in snapshot.bars:
        if bar.kind == "stock":
            histories[bar.symbol].append(bar.session_date)
    calculated = {
        key: {row.symbol if isinstance(analysis, MartinReport) else row["symbol"] for row in analysis.observations}
        for key, (_identifier, analysis) in scope.analyses.items()
    }
    selected = {
        selection.strategy: {row.symbol for row in selection.decision.items} for selection in scope.view.selections
    }
    result = []
    for company in company_views(snapshot, set(members)):
        symbol, classification = company.symbol, classified.get(company.symbol)
        result.append(
            UniverseStock(
                company=company,
                indices=tuple(sorted({row.index for row in members[symbol]})),
                industry=classification.industry if classification else None,
                themes=classification.themes if classification else (),
                history_rows=len(histories[symbol]),
                last_session=max(histories[symbol]) if histories[symbol] else None,
                strategies=tuple(
                    UniverseStrategy(
                        strategy=key,
                        data_eligible=symbol in snapshot.quality.eligible[key],
                        calculation_available=symbol in calculated[key],
                        in_pool=symbol in analysis.pool,
                        selected=symbol in selected[key] if key in selected else None,
                    )
                    for key, (_identifier, analysis) in scope.analyses.items()
                ),
                ref_prefix="universe/" + symbol,
            )
        )
    return tuple(result)


async def universe_page(store: RunStore, report_id: str, query: UniverseQuery) -> UniversePage:
    """只绑定已验证快照投影，LIKE元字符按字面匹配，分页不在内存切片。"""
    scope = await frozen_scope(store, None, report_id)
    stocks = universe_rows(scope)
    rows = func.json_array_elements(
        cast(bindparam("stocks", value=[row.model_dump(mode="json") for row in stocks], type_=JSON), JSON)
    ).table_valued(column("value", JSON))
    symbol = rows.c.value["company"]["symbol"].as_string()
    name = rows.c.value["company"]["name"].as_string()
    predicate = []
    if query.search is not None:
        predicate.append(
            or_(
                func.lower(symbol).contains(query.search.lower(), autoescape=True),
                func.lower(name).contains(query.search.lower(), autoescape=True),
            )
        )
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(rows).where(*predicate))
        values = (
            await connection.scalars(
                select(rows.c.value)
                .where(*predicate)
                .order_by(symbol)
                .limit(query.page_size)
                .offset((query.page - 1) * query.page_size)
            )
        ).all()
    return UniversePage(
        **report_scope(scope).model_dump(),
        universe_version=scope.snapshot.universe.version,
        classification_version=scope.snapshot.classifications.version,
        universe_total=len(stocks),
        items=tuple(UniverseStock(**value) for value in values),
        total=total,
        page=query.page,
        page_size=query.page_size,
        search=query.search,
        limitations=scope.snapshot.quality.limitations,
    )
