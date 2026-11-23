"""冻结报告上的市场、分组、比较与规则查询，不运行策略代理。"""

from typing import Literal

from sqlalchemy import JSON, bindparam, cast, column, func, select

from stock_research.analysis import MartinReport
from stock_research.errors import ResearchInputError
from stock_research.evidence import flatten_facts
from stock_research.identity import digest
from stock_research.observations import FrozenResearch, frozen_history, frozen_scope, stock_from_scope
from stock_research.quant_store import quant_state
from stock_research.queries import company_views
from stock_research.run_store import RunStore
from stock_research.schemas.observations import GroupObservation
from stock_research.schemas.report_queries import (
    BenchmarkHistory,
    ComparisonInput,
    FrozenRules,
    GroupPage,
    MarketOverview,
    QuantitativeScope,
    ReportScope,
    StockComparison,
    StrategyRules,
)


def report_scope(scope: FrozenResearch) -> ReportScope:
    """显式记录报告、运行和快照身份，消费方无需从文本推断截止日。"""
    return ReportScope(
        run_id=scope.view.run_id,
        report_id=scope.report_id,
        snapshot_id=scope.snapshot.id,
        snapshot_hash=scope.snapshot.content_hash,
        as_of=scope.snapshot.as_of,
        target_session=scope.view.target_session,
    )


async def market_overview(store: RunStore, report_id: str, limit: int) -> MarketOverview:
    """市场事实沿原Wyckoff计算；量化池顺序与正式初选分别保留。"""
    scope = await frozen_scope(store, None, report_id)
    facts, benchmarks, strategies = {}, [], []
    if "wyckoff" in scope.analyses:
        flatten_facts(scope.analyses["wyckoff"][1].metrics["benchmark"], "market/SPY", None, facts)
    for symbol in ("SPY", "QQQ", "IWM"):
        history, total = frozen_history(scope.snapshot, "benchmark", symbol, limit)
        benchmarks.append(BenchmarkHistory(symbol=symbol, history=history, history_total=total))
    for strategy in ("martin", "wyckoff"):
        if strategy not in scope.analyses:
            strategies.append(
                QuantitativeScope(
                    strategy=strategy,
                    artifact_id=None,
                    content_hash=None,
                    parameter_hash=digest(scope.manifest["parameters"][strategy]),
                    calculated_count=None,
                    exclusion_record_count=None,
                    pool=None,
                    quant=quant_state(scope.manifest, strategy),
                )
            )
            continue
        identifier, analysis = scope.analyses[strategy]
        # lint-redline:ignore SQL-02 此处set是证券集合，公司映射不构造SQL标识符。
        companies = {item.symbol: item for item in company_views(scope.snapshot, set(analysis.pool))}
        strategies.append(
            QuantitativeScope(
                strategy=strategy,
                artifact_id=identifier,
                content_hash=analysis.content_hash,
                parameter_hash=analysis.parameter_hash,
                calculated_count=len(analysis.observations),
                exclusion_record_count=len(analysis.excluded),
                pool=tuple(companies[symbol] for symbol in analysis.pool),
            )
        )
    return MarketOverview(
        **report_scope(scope).model_dump(),
        quality=scope.snapshot.quality,
        market_facts=tuple(facts.values()),
        benchmarks=tuple(benchmarks),
        strategies=tuple(strategies),
        limitations=scope.snapshot.quality.limitations,
    )


async def group_strength(
    store: RunStore,
    report_id: str,
    page: int,
    page_size: int,
    kind: Literal["industry", "theme"] | None = None,
    name: str | None = None,
) -> GroupPage:
    """完整校验后绑定原JSON，筛选、NULL排序和分页均由PostgreSQL执行。"""
    scope = await frozen_scope(store, None, report_id)
    if "martin" not in scope.analyses:
        state = quant_state(scope.manifest, "martin")
        return GroupPage(
            **report_scope(scope).model_dump(),
            items=None,
            total=None,
            page=page,
            page_size=page_size,
            quant=state,
            limitations=("本修订Martin量化尚不可用：" + (state.error_code or state.status),),
        )
    groups = scope.analyses["martin"][1].groups
    rows = func.json_array_elements(cast(bindparam("groups", value=groups, type_=JSON), JSON)).table_valued(
        column("value", JSON)
    )
    group_name = rows.c.value["name"].as_string()
    predicate = []
    if kind is not None:
        predicate.append(group_name.startswith(kind + ":"))
    if name is not None:
        predicate.append(group_name == name)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(rows).where(*predicate))
        values = (
            await connection.scalars(
                select(rows.c.value)
                .where(*predicate)
                .order_by(rows.c.value["score"].as_float().desc().nulls_last(), group_name)
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
        ).all()
    return GroupPage(
        **report_scope(scope).model_dump(),
        items=tuple(GroupObservation(**value, ref_prefix="group/" + digest(value["name"])[:16]) for value in values),
        total=total,
        page=page,
        page_size=page_size,
        limitations=scope.analyses["martin"][1].limitations,
    )


async def compare_stocks(store: RunStore, report_id: str, symbols: tuple[str, ...], limit: int) -> StockComparison:
    """整次比较共享相同修订和已验证输入；池外对象使整个请求失败。"""
    try:
        requested = ComparisonInput(symbols=symbols)
    except ValueError:
        raise ResearchInputError("比较须包含2～8个不同的大写规范代码") from None
    scope = await frozen_scope(store, None, report_id)
    return StockComparison(
        **report_scope(scope).model_dump(),
        items=tuple(stock_from_scope(scope, symbol, limit) for symbol in requested.symbols),
    )


async def strategy_rules(store: RunStore, report_id: str) -> StrategyRules:
    """从原产物投影参数和出处，不读取后来发布配置或当前生产默认值。"""
    scope = await frozen_scope(store, None, report_id)
    values = []
    for strategy in ("martin", "wyckoff"):
        if strategy not in scope.analyses:
            state = quant_state(scope.manifest, strategy)
            values.append(
                FrozenRules(
                    strategy=strategy,
                    artifact_id=None,
                    content_hash=None,
                    parameter_hash=digest(scope.manifest["parameters"][strategy]),
                    parameters=scope.manifest["parameters"][strategy],
                    factor_definitions={},
                    effective_config=None,
                    quant=state,
                    limitations=("本修订量化尚不可用：" + (state.error_code or state.status),),
                )
            )
            continue
        identifier, analysis = scope.analyses[strategy]
        values.append(
            FrozenRules(
                strategy=strategy,
                artifact_id=identifier,
                content_hash=analysis.content_hash,
                parameter_hash=analysis.parameter_hash,
                parameters=analysis.parameters,
                factor_definitions=analysis.factor_definitions if isinstance(analysis, MartinReport) else {},
                effective_config=None if isinstance(analysis, MartinReport) else analysis.effective_config,
                limitations=analysis.limitations,
            )
        )
    return StrategyRules(
        **report_scope(scope).model_dump(),
        source_hash=scope.manifest["source_hash"],
        prompt_hash=scope.manifest["prompt_hash"],
        strategies=tuple(values),
    )
