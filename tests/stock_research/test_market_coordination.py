"""原生PG验证研究复制与来源维护/写事务的互斥协议。"""

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import func, select

from stock_research.config import Settings
from stock_research.market_coordination import MARKET_DATA_KEY, US_MARKET_JOB_KEY, protect_market_copy
from stock_research.market_sync import SYNC_SCOPE, sync_market
from stock_research.market_writer import checkpoint, local_write_engine
from stock_research.store import close_engine, database_engine
from tests.stock_research.test_market_sync import seed_source, source_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


@pytest.mark.parametrize("key,shared", [(US_MARKET_JOB_KEY, False), (MARKET_DATA_KEY, True), (MARKET_DATA_KEY, False)])
def test_busy_source_cannot_advance_checkpoint_and_retries_after_owner_exits(
    postgres_settings: Settings, tmp_path: Path, key: int, shared: bool
) -> None:
    async def scenario() -> None:
        await seed_source(postgres_settings)
        path = tmp_path / "market.sqlite3"
        await sync_market(source_settings(postgres_settings), path)
        local = local_write_engine(path)
        source = database_engine(postgres_settings, test=True)
        try:
            before = await checkpoint(local, SYNC_SCOPE)
            async with source.begin() as owner:
                function = func.pg_advisory_xact_lock_shared if shared else func.pg_advisory_xact_lock
                await owner.execute(select(function(key)))
                with pytest.raises(ValueError, match="正在运行"):
                    await sync_market(source_settings(postgres_settings), path)
                assert await checkpoint(local, SYNC_SCOPE) == before
            result = await sync_market(source_settings(postgres_settings), path)
            assert result["rows"] == 0 and result["issues"] == 0
            assert await checkpoint(local, SYNC_SCOPE) > before
        finally:
            await close_engine(source)
            await close_engine(local)

    asyncio.run(scenario())


def test_copy_excludes_jobs_and_shared_writes_until_transaction_closes(postgres_settings: Settings) -> None:
    async def scenario() -> None:
        source = database_engine(postgres_settings, test=True)
        try:
            async with source.begin() as copy:
                await protect_market_copy(copy)
                async with source.begin() as writer:
                    assert not await writer.scalar(select(func.pg_try_advisory_xact_lock_shared(MARKET_DATA_KEY)))
                    assert not await writer.scalar(select(func.pg_try_advisory_xact_lock(US_MARKET_JOB_KEY)))
            async with source.begin() as writer:
                assert await writer.scalar(select(func.pg_try_advisory_xact_lock_shared(MARKET_DATA_KEY)))
                assert await writer.scalar(select(func.pg_try_advisory_xact_lock(US_MARKET_JOB_KEY)))
        finally:
            await close_engine(source)

    asyncio.run(scenario())
