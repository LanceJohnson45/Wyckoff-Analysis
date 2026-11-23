"""当前观察投影与日事件同事务保存，失败标记不抹去最后有效状态。"""

from datetime import date

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.analysis import MartinReport
from stock_research.identity import digest, new_id
from stock_research.models import Snapshot
from stock_research.observations import frozen_analyses
from stock_research.queries import company_views
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import WATCHLIST_DAYS, WATCHLIST_EVENTS, WATCHLIST_ITEMS
from stock_research.schemas.watchlists import WatchItem
from stock_research.store import load_snapshot
from stock_research.watch_policy import assess_martin, assess_wyckoff
from stock_research.wyckoff import WyckoffReport


async def tracking_inputs(store: RunStore, body: dict[str, object]) -> tuple[Snapshot, dict[str, object]]:
    """跟踪绑定同一已验证快照及量化产物，不重新计算技术状态。"""
    snapshot = await load_snapshot(store.engine, store.workspace, body["manifest"]["snapshot_id"])
    analyses = await frozen_analyses(store, body["manifest"], snapshot)
    return snapshot, {key: report for key, (_identifier, report) in analyses.items()}


def next_watch_item(
    previous: WatchItem | None,
    *,
    symbol: str,
    name: str,
    strategy: str,
    selected: bool,
    selection_available: bool,
    analysis: MartinReport | WyckoffReport,
    run_id: str,
    snapshot: Snapshot,
    target: date,
) -> WatchItem:
    """新日结构观察来自原事实；缺模型/数据时保持旧的状态与证据来源。"""
    assessment = assess_martin(analysis, symbol) if strategy == "martin" else assess_wyckoff(analysis, symbol)
    health = dict(
        target_session=target,
        checked_run_id=run_id,
        data_insufficient=not assessment.available,
        not_reviewed=not selection_available or not assessment.reviewed,
    )
    if previous and (health["data_insufficient"] or health["not_reviewed"]):
        return WatchItem.model_validate(
            {
                **previous.model_dump(),
                **health,
                "reason": assessment.reason if not assessment.reviewed else "selection_not_committed",
            }
        )
    fresh = previous is None or previous.state == "invalid"
    state = (
        "new" if selected and fresh else "invalid" if not assessment.valid else "near" if assessment.near else "waiting"
    )
    if previous and previous.state_session == target and previous.state == "new" and selected:
        state = "new"
    return WatchItem(
        symbol=symbol,
        name=name,
        strategy=strategy,
        state=state,
        state_run_id=run_id,
        state_snapshot_id=snapshot.id,
        state_as_of=snapshot.as_of,
        state_session=target,
        reason="new_selection" if state == "new" else assessment.reason,
        evidence_refs=assessment.refs,
        **health,
    )


async def persist_item(
    connection: AsyncConnection, workspace: str, item: WatchItem, previous: WatchItem | None
) -> None:
    """事件业务键去重，投影和事件由外部同一事务提交。"""
    payload = item.model_dump(mode="json")
    statement = insert(WATCHLIST_ITEMS).values(
        uuid=new_id(),
        workspace=workspace,
        strategy=item.strategy,
        symbol=item.symbol,
        target_session=item.target_session,
        payload=payload,
        content_hash=digest(payload),
    )
    await connection.execute(
        statement.on_conflict_do_update(
            index_elements=["workspace", "strategy", "symbol"],
            set_={
                "target_session": item.target_session,
                "payload": payload,
                "content_hash": digest(payload),
                "update_time": func.now(),
            },
        )
    )
    kind = (
        f"state:{item.state}"
        if previous is None or previous.state != item.state
        else "health:insufficient"
        if item.data_insufficient
        else "health:unreviewed"
        if item.not_reviewed
        else "health:reviewed"
    )
    if previous and item.model_dump(exclude={"checked_run_id"}) == previous.model_dump(exclude={"checked_run_id"}):
        return
    event = dict(before=previous.state if previous else None, after=payload)
    await connection.execute(
        insert(WATCHLIST_EVENTS)
        .values(
            uuid=new_id(),
            workspace=workspace,
            strategy=item.strategy,
            symbol=item.symbol,
            target_session=item.target_session,
            event_kind=kind,
            run_uuid=str(item.checked_run_id),
            payload=event,
            content_hash=digest(event),
        )
        .on_conflict_do_nothing(index_elements=["workspace", "strategy", "symbol", "target_session", "event_kind"])
    )


async def apply_tracking(
    connection: AsyncConnection,
    workspace: str,
    body: dict[str, object],
    snapshot: Snapshot,
    analyses: dict[str, object],
) -> None:
    """workspace锁串行化跨run更新，旧日期不覆盖当前观察名单。"""
    key = int(digest({"watchlists": workspace})[:16], 16) - (1 << 63)
    await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
    await connection.execute(select(func.pg_advisory_xact_lock(key)))
    target = date.fromisoformat(body["manifest"]["target_session"])
    latest = await connection.scalar(
        select(func.max(WATCHLIST_ITEMS.c.target_session)).where(WATCHLIST_ITEMS.c.workspace == workspace)
    )
    latest_day = await connection.scalar(
        select(func.max(WATCHLIST_DAYS.c.target_session)).where(WATCHLIST_DAYS.c.workspace == workspace)
    )
    if any(day is not None and day > target for day in (latest, latest_day)):
        return
    selections = {row["strategy"]: row["payload"]["decision"]["items"] for row in body["selections"]}
    for strategy, analysis in analyses.items():
        await update_strategy(
            connection, workspace, body["run_id"], target, snapshot, strategy, analysis, selections.get(strategy)
        )


async def update_strategy(
    connection: AsyncConnection,
    workspace: str,
    run_id: str,
    target: date,
    snapshot: Snapshot,
    strategy: str,
    analysis: MartinReport | WyckoffReport,
    selection: list[dict[str, object]] | None,
) -> None:
    """第一份成功初选固定当日来源，强制重分析保留在报告中供对照。"""
    day_predicate = (
        WATCHLIST_DAYS.c.workspace == workspace,
        WATCHLIST_DAYS.c.strategy == strategy,
        WATCHLIST_DAYS.c.target_session == target,
    )
    owner = (await connection.execute(select(WATCHLIST_DAYS).where(*day_predicate))).mappings().one_or_none()
    if owner:
        verified_payload(dict(owner))
        if owner["run_uuid"] != run_id:
            return
    elif selection is not None:
        payload = {"run_id": run_id, "policy_version": "watch-v1"}
        await connection.execute(
            insert(WATCHLIST_DAYS).values(
                uuid=new_id(),
                workspace=workspace,
                strategy=strategy,
                target_session=target,
                run_uuid=run_id,
                payload=payload,
                content_hash=digest(payload),
            )
        )
    rows = (
        (
            await connection.execute(
                select(WATCHLIST_ITEMS).where(
                    WATCHLIST_ITEMS.c.workspace == workspace,
                    WATCHLIST_ITEMS.c.strategy == strategy,
                    WATCHLIST_ITEMS.c.is_deleted.is_(False),
                )
            )
        )
        .mappings()
        .all()
    )
    previous = {row["symbol"]: WatchItem.model_validate(verified_payload(dict(row))) for row in rows}
    selected = {item["symbol"] for item in selection or []}
    names = {item.symbol: item.name for item in company_views(snapshot, selected)}
    symbols = selected | {symbol for symbol, item in previous.items() if item.state != "invalid"}
    for symbol in sorted(symbols):
        item = next_watch_item(
            previous.get(symbol),
            symbol=symbol,
            name=names.get(symbol) or previous[symbol].name,
            strategy=strategy,
            selected=symbol in selected,
            selection_available=selection is not None,
            analysis=analysis,
            run_id=run_id,
            snapshot=snapshot,
            target=target,
        )
        await persist_item(connection, workspace, item, previous.get(symbol))
