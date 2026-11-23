"""SQLAlchemy async 数据存取；事务提交后才返回持久化快照。"""

import asyncio

from sqlalchemy import Table, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine
from sqlalchemy.schema import CreateSchema

from stock_research.config import Settings
from stock_research.identity import digest, new_id, now
from stock_research.models import ClassificationVersion, Snapshot, UniverseVersion
from stock_research.schema import (
    CLASSIFICATION_VERSIONS,
    GROUP_MEMBERS,
    MARKET_SNAPSHOTS,
    RESEARCH_METADATA,
    SCHEMA_VERSION,
    UNIVERSE_MEMBERS,
    UNIVERSES,
)


def database_engine(settings: Settings, *, test: bool = False) -> AsyncEngine:
    """限制连接和语句时间；测试连接只能使用派生的 _test 库。"""
    return create_async_engine(
        settings.database_url(test=test),
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        connect_args={"timeout": 5, "command_timeout": 30, "server_settings": {"timezone": "Asia/Shanghai"}},
    )


async def initialize_schema(engine: AsyncEngine) -> None:
    """显式初始化数据阶段 v1，不在读取或保存时隐式修改 schema。"""
    async with engine.begin() as connection:
        await connection.execute(CreateSchema("stock_research", if_not_exists=True))
        await connection.run_sync(RESEARCH_METADATA.create_all)


async def save_version(
    connection: AsyncConnection,
    workspace: str,
    table: Table,
    version: UniverseVersion | ClassificationVersion,
) -> str:
    """复用完全相同的版本；同名版本内容变化使整个事务回滚。"""
    payload = version.model_dump(mode="json")
    content_hash = digest(payload)
    statement = insert(table).values(
        uuid=new_id(),
        workspace=workspace,
        version=version.version,
        content_hash=content_hash,
        payload=payload,
        create_time=now(),
        update_time=now(),
    )
    created = await connection.scalar(
        statement.on_conflict_do_nothing(index_elements=["workspace", "version"]).returning(table.c.uuid)
    )
    row = (
        await connection.execute(
            select(table.c.uuid, table.c.content_hash).where(
                table.c.workspace == workspace,
                table.c.version == version.version,
            )
        )
    ).one()
    if row.content_hash != content_hash:
        raise ValueError("数据库已有同名不同内容的版本，请发布新版本")
    if created:
        await save_members(connection, row.uuid, version)
    return row.uuid


async def save_members(
    connection: AsyncConnection, version_id: str, version: UniverseVersion | ClassificationVersion
) -> None:
    """保存成员可查询列与来源全文，缺失行业不生成伪分组。"""
    if isinstance(version, UniverseVersion):
        rows = [
            dict(
                uuid=new_id(),
                universe_uuid=version_id,
                symbol=m.symbol,
                index=m.index,
                effective_date=m.effective_date,
                expiry_date=m.expiry_date,
                payload=m.model_dump(mode="json"),
            )
            for m in version.members
        ]
        table = UNIVERSE_MEMBERS
    else:
        rows = [
            dict(
                uuid=new_id(),
                classification_uuid=version_id,
                symbol=r.symbol,
                kind=kind,
                name=name,
                effective_date=r.effective_date,
                payload=r.model_dump(mode="json"),
            )
            for r in version.records
            for kind, name in (("industry", r.industry), *(("theme", t) for t in r.themes))
            if name and name.strip()
        ]
        table = GROUP_MEMBERS
    if rows:
        await connection.execute(insert(table), rows)


async def save_snapshot(engine: AsyncEngine, workspace: str, snapshot: Snapshot) -> Snapshot:
    """版本与快照同事务保存，并发重试由数据库唯一约束归并。"""
    snapshot = Snapshot.model_validate_json(snapshot.model_dump_json())
    async with engine.begin() as connection:
        universe_id = await save_version(connection, workspace, UNIVERSES, snapshot.universe)
        classification_id = await save_version(connection, workspace, CLASSIFICATION_VERSIONS, snapshot.classifications)
        statement = insert(MARKET_SNAPSHOTS).values(
            uuid=snapshot.id,
            workspace=workspace,
            market="us",
            as_of=snapshot.as_of,
            schema_version=SCHEMA_VERSION,
            universe_uuid=universe_id,
            classification_uuid=classification_id,
            content_hash=snapshot.content_hash,
            payload=snapshot.model_dump(mode="json"),
            create_time=snapshot.create_time,
            update_time=now(),
        )
        await connection.execute(statement.on_conflict_do_nothing(index_elements=["workspace", "content_hash"]))
        snapshot_id = await connection.scalar(
            select(MARKET_SNAPSHOTS.c.uuid).where(
                MARKET_SNAPSHOTS.c.workspace == workspace,
                MARKET_SNAPSHOTS.c.content_hash == snapshot.content_hash,
            )
        )
    return await load_snapshot(engine, workspace, snapshot_id)


async def load_snapshot(engine: AsyncEngine, workspace: str, snapshot_id: str) -> Snapshot:
    """独立连接重读且重新验证内容摘要，跨 workspace 不返回数据。"""
    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    select(MARKET_SNAPSHOTS).where(
                        MARKET_SNAPSHOTS.c.workspace == workspace,
                        MARKET_SNAPSHOTS.c.uuid == snapshot_id,
                        MARKET_SNAPSHOTS.c.is_deleted.is_(False),
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
    if row is None:
        raise ValueError("快照不存在或不属于当前 workspace")
    snapshot = Snapshot.model_validate(row["payload"])
    if row["schema_version"] != SCHEMA_VERSION or snapshot.content_hash != row["content_hash"]:
        raise ValueError("快照存储版本或摘要不一致")
    return snapshot


async def database_ready(engine: AsyncEngine) -> bool:
    """同时检查连接和数据阶段 schema 可读性，不执行 DDL。"""
    async with engine.connect() as connection:
        await connection.scalar(select(func.count()).select_from(MARKET_SNAPSHOTS))
    return True


async def close_engine(engine: AsyncEngine) -> None:
    """即使数据库异常，连接池关闭也有确定期限。"""
    async with asyncio.timeout(5):
        await engine.dispose()
