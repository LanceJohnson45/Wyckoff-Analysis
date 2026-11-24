"""与来源工作树兼容的 SQLite 增量写入与异常隔离。"""

from datetime import datetime
from pathlib import Path

from sqlalchemy import URL, delete, or_, select, tuple_
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from stock_research.data import DAILY_BARS, SOURCE_CONFIG, SOURCE_ISSUES, SOURCE_METADATA, SYNC_STATE
from stock_research.identity import SHANGHAI


async def initialize_local(engine: AsyncEngine, source_id: str) -> None:
    """绑定来源身份，不能把另一云端数据混入已有行情文件。"""
    async with engine.begin() as connection:
        await connection.run_sync(SOURCE_METADATA.create_all)
        await connection.execute(
            insert(SOURCE_CONFIG).values(key="cloud_source", value=source_id).on_conflict_do_nothing()
        )
        current = await connection.scalar(select(SOURCE_CONFIG.c.value).where(SOURCE_CONFIG.c.key == "cloud_source"))
        if current != source_id:
            raise ValueError("本地行情文件属于其他云端数据库，请使用独立路径")


def local_write_engine(path: Path) -> AsyncEngine:
    """异步连接仅在显式同步时启用，不被快照读取路径使用。"""
    return create_async_engine(
        URL.create("sqlite+aiosqlite", database=str(path.expanduser().resolve())), connect_args={"timeout": 30}
    )


async def checkpoint(engine: AsyncEngine, scope: str) -> datetime | None:
    """读取成功完成的独立范围水位；旧SPY-only范围不能复用。"""
    async with engine.connect() as connection:
        value = await connection.scalar(select(SYNC_STATE.c.checkpoint).where(SYNC_STATE.c.scope == scope))
    if not value:
        return None
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("同步水位必须带时区，请检查来源文件")
    return result


async def write_batch(engine: AsyncEngine, bars: list[dict[str, object]], issues: list[dict[str, object]]) -> None:
    """行情写入、旧隔离清除与新隔离同批原子提交。"""
    async with engine.begin() as connection:
        if bars:
            await write_valid(connection, bars)
        if issues:
            await write_issues(connection, issues)


async def write_valid(connection: AsyncConnection, bars: list[dict[str, object]]) -> None:
    """旧修订不可覆盖新行情，修复行按版本清除隔离。"""
    statement = insert(DAILY_BARS)
    await connection.execute(
        statement.on_conflict_do_update(
            index_elements=["market", "kind", "symbol", "adjust", "date"],
            set_={
                name: getattr(statement.excluded, name)
                for name in ("open", "high", "low", "close", "volume", "cloud_updated_at", "imported_at")
            },
            where=(statement.excluded.cloud_updated_at >= DAILY_BARS.c.cloud_updated_at)
            & or_(
                statement.excluded.cloud_updated_at > DAILY_BARS.c.cloud_updated_at,
                *(
                    getattr(statement.excluded, name).is_distinct_from(DAILY_BARS.c[name])
                    for name in ("open", "high", "low", "close", "volume")
                ),
            ),
        ),
        bars,
    )
    versions = {(b["market"], b["kind"], b["symbol"], b["date"]): b["cloud_updated_at"] for b in bars}
    previous = (
        (
            await connection.execute(
                select(SOURCE_ISSUES).where(
                    SOURCE_ISSUES.c.market == "us",
                    SOURCE_ISSUES.c.symbol.in_({b["symbol"] for b in bars}),
                )
            )
        )
        .mappings()
        .all()
    )
    resolved = [
        key
        for issue in previous
        if (key := (issue["market"], issue["kind"], issue["symbol"], issue["date"])) in versions
        and versions[key] >= issue["cloud_updated_at"]
    ]
    if resolved:
        await connection.execute(
            delete(SOURCE_ISSUES).where(
                tuple_(
                    SOURCE_ISSUES.c.market,
                    SOURCE_ISSUES.c.kind,
                    SOURCE_ISSUES.c.symbol,
                    SOURCE_ISSUES.c.date,
                ).in_(resolved)
            )
        )


async def write_issues(connection: AsyncConnection, issues: list[dict[str, object]]) -> None:
    """只有不早于已存行情的新错误可进入隔离表。"""
    existing = (
        (
            await connection.execute(
                select(DAILY_BARS).where(
                    DAILY_BARS.c.market == "us",
                    DAILY_BARS.c.symbol.in_({i["symbol"] for i in issues}),
                )
            )
        )
        .mappings()
        .all()
    )
    versions = {(b["market"], b["kind"], b["symbol"], b["date"]): b["cloud_updated_at"] for b in existing}
    pending = [
        i for i in issues if versions.get((i["market"], i["kind"], i["symbol"], i["date"]), "") <= i["cloud_updated_at"]
    ]
    if pending:
        statement = insert(SOURCE_ISSUES)
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=["market", "kind", "symbol", "date"],
                set_={name: getattr(statement.excluded, name) for name in ("cloud_updated_at", "reason")},
                where=statement.excluded.cloud_updated_at >= SOURCE_ISSUES.c.cloud_updated_at,
            ),
            pending,
        )


async def complete(engine: AsyncEngine, scope: str, stamp: datetime) -> None:
    """全量传输成功后才推进水位，并发旧完成记录不能让水位倒退。"""
    statement = insert(SYNC_STATE).values(scope=scope, market="us", checkpoint=stamp.astimezone(SHANGHAI).isoformat())
    async with engine.begin() as connection:
        await connection.execute(
            statement.on_conflict_do_update(
                index_elements=["scope"],
                set_={"checkpoint": statement.excluded.checkpoint},
                where=statement.excluded.checkpoint > SYNC_STATE.c.checkpoint,
            )
        )
