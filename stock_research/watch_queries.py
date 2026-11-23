"""名单和事件在PostgreSQL分页，不在客户端全量切片。"""

from sqlalchemy import Table, func, select

from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import WATCHLIST_EVENTS, WATCHLIST_ITEMS
from stock_research.schemas.api import Strategy
from stock_research.schemas.watchlists import WatchEvent, WatchEventPage, WatchItem, WatchPage


async def watch_rows(
    store: RunStore, table: Table, page: int, page_size: int, strategy: Strategy | None, symbol: str | None = None
) -> tuple[list[dict[str, object]], int]:
    """相同日期使用strategy/symbol/UUID打破平局，表仅由内部调用者选择。"""
    predicate = [table.c.workspace == store.workspace, table.c.is_deleted.is_(False)]
    if strategy:
        predicate.append(table.c.strategy == strategy)
    if symbol:
        predicate.append(table.c.symbol == symbol)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(table).where(*predicate))
        rows = (
            (
                await connection.execute(
                    select(table)
                    .where(*predicate)
                    .order_by(table.c.target_session.desc(), table.c.strategy, table.c.symbol, table.c.uuid)
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    return [dict(row) for row in rows], total


async def watch_page(store: RunStore, page: int, page_size: int, strategy: Strategy | None = None) -> WatchPage:
    """健康标记与最后有效状态同时返回，不把失败作为失效。"""
    rows, total = await watch_rows(store, WATCHLIST_ITEMS, page, page_size, strategy)
    return WatchPage(
        items=tuple(WatchItem.model_validate(verified_payload(row)) for row in rows),
        total=total,
        page=page,
        page_size=page_size,
    )


async def event_page(
    store: RunStore, page: int, page_size: int, strategy: Strategy | None = None, symbol: str | None = None
) -> WatchEventPage:
    """事件保留其写入时的before/after，不用当前投影覆盖。"""
    rows, total = await watch_rows(store, WATCHLIST_EVENTS, page, page_size, strategy, symbol)
    items = []
    for row in rows:
        payload = verified_payload(row)
        items.append(
            WatchEvent(
                id=row["uuid"],
                run_id=row["run_uuid"],
                target_session=row["target_session"],
                strategy=row["strategy"],
                symbol=row["symbol"],
                kind=row["event_kind"],
                before=payload["before"],
                after=payload["after"],
            )
        )
    return WatchEventPage(items=tuple(items), total=total, page=page, page_size=page_size)
