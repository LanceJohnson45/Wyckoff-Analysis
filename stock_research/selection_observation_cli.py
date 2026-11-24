"""固定报告正式初选的CLI观测，不调用模型、供应商或通知。"""

import asyncio
from datetime import date

from pydantic import UUID4, TypeAdapter

from core.trade_friction import round_trip_cost_pct
from stock_research.artifacts import data_lock, write_once
from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import Settings
from stock_research.data import read_market
from stock_research.evaluation_outcomes import LONGEST_HORIZON, indexed_outcomes, observation_sessions, price_index
from stock_research.identity import canonical_json, digest
from stock_research.run_store import RunStore
from stock_research.schemas.selection_observations import (
    SelectionObservationInput,
    SelectionObservationReport,
    SelectionOutcome,
    SelectionScope,
)
from stock_research.selection_scope import selection_scope
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.store import close_engine, database_engine

MAX_SELECTION_ROWS = 11 * 2 * LONGEST_HORIZON
MAX_OBSERVATION_FILE_BYTES = 1024 * 1024
LIMITATIONS = (
    "正式初选的描述性开盘参考观测，不模拟订单、持仓或实际成交。",
    "参考点是不早于目标日且严格晚于初选提交时刻的首个XNYS开盘；晚到初选顺延。",
    "日线为当前修订的前复权价格，不是严格历史时点价格；成本是工程估算。",
    "报告修订和同日重分析不是独立历史样本；未完成或缺数窗口保留，不缩小分母。",
    "本产物不表示人工审查完成，不构成参数优化或收益有效性证明。",
)


def observation_input(settings: Settings, scope: SelectionScope, cutoff: date) -> SelectionObservationInput:
    """SQL只取正式名单和其窗口，先过滤再应用行数上限。"""
    symbols = sorted({symbol for row in scope.selections for symbol in row.symbols})
    days = sorted(
        {
            day
            for row in scope.selections
            if row.symbols
            for day in observation_sessions(row.reference_session)[:LONGEST_HORIZON]
            if day <= cutoff
        }
    )
    instruments = tuple(("stock", symbol) for symbol in symbols) + (("benchmark", "SPY"),)
    source, bars, issues = read_market(
        settings.local_market_path,
        days[0] if days else cutoff,
        days[-1] if days else cutoff,
        max_rows=MAX_SELECTION_ROWS,
        instruments=instruments,
        session_dates=days,
    )
    return SelectionObservationInput(
        scope=scope,
        cutoff=cutoff,
        source_hash=LOADED_SOURCE_IDENTITY,
        source_identity=source,
        costs={symbol: round_trip_cost_pct(code=symbol + ".US") for symbol in symbols},
        bars=bars,
        issues=issues,
    )


def observation_report(inputs: SelectionObservationInput) -> SelectionObservationReport:
    """同一股票的双策略归属分别保留，固定参考点与SPY窗口对齐。"""
    prices = price_index(inputs.bars)
    for issue in inputs.issues:
        prices.pop((issue.kind, issue.symbol, date.fromisoformat(issue.reason.rsplit(":", 1)[-1])), None)
    samples = tuple(
        SelectionOutcome(
            selection_id=row.selection_id,
            strategy=row.strategy,
            symbol=symbol,
            windows=indexed_outcomes(symbol, row.reference_session, inputs.cutoff, prices, reference="forward_open"),
        )
        for row in inputs.scope.selections
        for symbol in row.symbols
    )
    if any(window.cost_pct != inputs.costs[row.symbol] for row in samples for window in row.windows):
        raise ValueError("正式初选观测成本与冻结输入不一致")
    body = dict(
        kind="formal_selection_observations",
        algorithm="selection-forward-open-v1",
        reference="first_open_after_commit",
        scope=inputs.scope.model_dump(mode="json"),
        cutoff=inputs.cutoff.isoformat(),
        source_hash=inputs.source_hash,
        input_hash=digest(inputs.model_dump(mode="json")),
        source_identity=inputs.source_identity,
        costs=inputs.costs,
        samples=[row.model_dump(mode="json") for row in samples],
        limitations=LIMITATIONS,
    )
    return SelectionObservationReport(**body, content_hash=digest(body))


def observe_fixed_scope(settings: Settings, scope: SelectionScope, cutoff: date) -> dict[str, object]:
    """显式CLI计算后原子发布；所有文件重读校验，原报告不写入。"""
    ensure_source_current()
    with data_lock(settings.artifacts_path / "locks", "selection-observations", str(scope.report_id)):
        inputs = observation_input(settings, scope, cutoff)
        report = observation_report(inputs)
        root = settings.artifacts_path / "selection-observations" / report.input_hash
        request = {**inputs.model_dump(mode="json", exclude={"bars", "issues"}), "input_hash": report.input_hash}
        files = {
            "request.json": canonical_json(request) + "\n",
            "input.json": inputs.model_dump_json() + "\n",
            "report.json": report.model_dump_json() + "\n",
        }
        for name, content in files.items():
            path = root / name
            if (
                len(content.encode()) > MAX_OBSERVATION_FILE_BYTES
                or path.exists()
                and path.stat().st_size > MAX_OBSERVATION_FILE_BYTES
            ):
                raise ValueError("正式初选观测文件超出上限")
            write_once(path, content)
        loaded = SelectionObservationInput.model_validate_json((root / "input.json").read_text(encoding="utf-8"))
        verified = SelectionObservationReport.model_validate_json((root / "report.json").read_text(encoding="utf-8"))
        if digest(loaded.model_dump(mode="json")) != verified.input_hash or verified != report:
            raise ValueError("正式初选观测文件绑定不一致")
        ensure_source_current()
    return {
        "kind": verified.kind,
        "path": str((root / "report.json").resolve()),
        "content_hash": verified.content_hash,
        "input_hash": verified.input_hash,
        "report_id": str(scope.report_id),
        "cutoff": str(cutoff),
        "selection_count": len(scope.selections),
        "observation_count": len(verified.samples),
        "missing_strategies": sorted({"martin", "wyckoff"} - {row.strategy for row in scope.selections}),
        "manual_review": "pending",
        "limitations": verified.limitations,
    }


async def observe_selections_command(settings: Settings, report_id: str, cutoff: date) -> dict[str, object]:
    """原生库只读绑定完成后关闭连接，文件与同步行情处理不阻塞事件循环。"""
    ensure_source_current()
    identifier = str(TypeAdapter(UUID4).validate_python(report_id))
    sessions_ending(cutoff, 1)
    if cutoff > latest_complete_session():
        raise ValueError("观测截止日尚未完成正式收盘")
    engine = database_engine(settings)
    try:
        scope = await selection_scope(RunStore(engine, settings.workspace), identifier)
    finally:
        await close_engine(engine)
    return await asyncio.to_thread(observe_fixed_scope, settings, scope, cutoff)
