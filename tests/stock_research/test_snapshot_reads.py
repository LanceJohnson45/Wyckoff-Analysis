"""真实PG验证冻结快照的传输、隔离和缓存失效。"""

import asyncio

import pytest
from sqlalchemy import event, update

from stock_research.config import Settings
from stock_research.models import Snapshot
from stock_research.schema import MARKET_SNAPSHOTS
from stock_research.store import close_engine, database_engine, initialize_schema, load_snapshot, save_snapshot

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def test_normal_concurrent_reads_transfer_payload_once(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【正常值】冷启动并发及后续热读只传输一份完整快照。"""

    async def scenario() -> None:
        writer = database_engine(postgres_settings, test=True)
        reader = database_engine(postgres_settings, test=True)
        statements = []
        event.listen(reader.sync_engine, "before_cursor_execute", lambda c, q, s, p, x, m: statements.append(s))
        try:
            await initialize_schema(writer)
            saved = await save_snapshot(writer, "read-once", snapshot)

            results = await asyncio.gather(*(load_snapshot(reader, "read-once", saved.id) for _ in range(8)))
            warm = await load_snapshot(reader, "read-once", saved.id)

            assert all(result == saved for result in (*results, warm))
            transfers = [s for s in statements if "market_snapshots.payload" in s]
            assert len(transfers) == 1, f"完整快照传输了{len(transfers)}次"
        finally:
            await close_engine(reader)
            await close_engine(writer)

    asyncio.run(scenario())


def test_boundary_oversized_snapshot_is_not_retained(
    postgres_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【边界值】超过缓存字节上限的合法输入仍可读，不永久占用缓存。"""
    monkeypatch.setattr("stock_research.store.SNAPSHOT_CACHE_MAX_BYTES", 1)

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        statements = []
        try:
            await initialize_schema(engine)
            saved = await save_snapshot(engine, "read-size", snapshot)
            event.listen(engine.sync_engine, "before_cursor_execute", lambda c, q, s, p, x, m: statements.append(s))

            values = [await load_snapshot(engine, "read-size", saved.id) for _ in range(2)]

            assert all(value == saved for value in values)
            assert len([s for s in statements if "market_snapshots.payload" in s]) == 2
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_cached_snapshot_detects_payload_tampering(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【异常值】即使篡改保留时间戳，热缓存仍不得遮住坏摘要。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            saved = await save_snapshot(engine, "read-tamper", snapshot)
            payload = saved.model_dump(mode="json")
            payload["source_identity"] = "tampered"
            async with engine.begin() as connection:
                await connection.execute(
                    update(MARKET_SNAPSHOTS)
                    .where(MARKET_SNAPSHOTS.c.uuid == saved.id)
                    .values(payload=payload, update_time=MARKET_SNAPSHOTS.c.update_time)
                )

            with pytest.raises(ValueError, match="摘要"):
                await load_snapshot(engine, "read-tamper", saved.id)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_cached_snapshot_rechecks_scope_and_deletion(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【异常值】热缓存不能绕过workspace或软删除。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            saved = await save_snapshot(engine, "read-scope", snapshot)
            with pytest.raises(ValueError, match="workspace"):
                await load_snapshot(engine, "other", saved.id)
            async with engine.begin() as connection:
                await connection.execute(
                    update(MARKET_SNAPSHOTS).where(MARKET_SNAPSHOTS.c.uuid == saved.id).values(is_deleted=True)
                )

            with pytest.raises(ValueError, match="workspace"):
                await load_snapshot(engine, "read-scope", saved.id)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_readers_cannot_mutate_cached_quality(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【正常值】领域对象里的可变字典不会串到下一次查询。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            saved = await save_snapshot(engine, "read-copy", snapshot)
            saved.quality.eligible.clear()

            loaded = await load_snapshot(engine, "read-copy", saved.id)

            assert loaded.quality.eligible == snapshot.quality.eligible
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
