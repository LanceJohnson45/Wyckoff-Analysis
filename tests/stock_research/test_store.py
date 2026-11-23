"""真实 PostgreSQL 的事务、版本不可变和并发存取验证。"""

import asyncio
import shutil
import subprocess

import pytest
from sqlalchemy import func, select, text

from stock_research.config import Settings
from stock_research.models import Snapshot
from stock_research.quality import build_snapshot
from stock_research.schema import (
    CLASSIFICATION_VERSIONS,
    GROUP_MEMBERS,
    MARKET_SNAPSHOTS,
    UNIVERSE_MEMBERS,
    UNIVERSES,
    schema_ddl,
)
from stock_research.store import close_engine, database_engine, initialize_schema, load_snapshot, save_snapshot

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def test_normal_maintained_ddl_applies_to_postgres(postgres_settings: Settings) -> None:
    """【正常值】维护的DDL可在真实隔离PostgreSQL执行，不依赖create_all。"""
    psql = shutil.which("psql")
    if not psql:
        pytest.skip("本机没有 psql，无法执行维护DDL的集成验收")

    result = subprocess.run(
        [
            psql,
            "-X",
            "-h",
            postgres_settings.postgres_host,
            "-U",
            postgres_settings.postgres_user,
            "-d",
            postgres_settings.database_url(test=True).database,
            "--set",
            "ON_ERROR_STOP=on",
        ],
        input=schema_ddl(),
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert "COMMIT" in result.stdout


def test_normal_initialize_existing_schema_without_database_create(postgres_settings: Settings) -> None:
    """【正常值】仅有schema权限的账号不需要数据库CREATE权限即可初始化。"""

    async def scenario() -> None:
        admin = database_engine(postgres_settings, test=True)
        restricted = database_engine(
            postgres_settings.model_copy(update={"postgres_user": "limited_research"}), test=True
        )
        try:
            await initialize_schema(admin)
            async with admin.begin() as connection:
                await connection.execute(text("CREATE ROLE limited_research LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE"))
                await connection.execute(text("GRANT USAGE, CREATE ON SCHEMA stock_research TO limited_research"))

            await initialize_schema(restricted)
            async with restricted.connect() as connection:
                can_create = await connection.scalar(
                    select(func.has_database_privilege(func.current_user(), func.current_database(), "CREATE"))
                )

            assert not can_create
        finally:
            await close_engine(restricted)
            await close_engine(admin)

    asyncio.run(scenario())


def test_idempotent_concurrent_snapshot_commits_once(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【幂等性】两个连接同时保存相同快照，只提交一行且成员可独立查出。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            saved = await asyncio.gather(*(save_snapshot(engine, "concurrent", snapshot) for _ in range(2)))
            async with engine.connect() as connection:
                database = await connection.scalar(select(func.current_database()))
                count = await connection.scalar(
                    select(func.count())
                    .select_from(MARKET_SNAPSHOTS)
                    .where(MARKET_SNAPSHOTS.c.workspace == "concurrent")
                )
                members = await connection.scalar(
                    select(func.count())
                    .select_from(UNIVERSE_MEMBERS)
                    .join(UNIVERSES, UNIVERSE_MEMBERS.c.universe_uuid == UNIVERSES.c.uuid)
                    .where(UNIVERSES.c.workspace == "concurrent")
                )
                groups = await connection.scalar(
                    select(func.count())
                    .select_from(GROUP_MEMBERS)
                    .join(
                        CLASSIFICATION_VERSIONS, GROUP_MEMBERS.c.classification_uuid == CLASSIFICATION_VERSIONS.c.uuid
                    )
                    .where(CLASSIFICATION_VERSIONS.c.workspace == "concurrent")
                )

            assert database.endswith("_test")
            assert count == 1
            assert saved[0].id == saved[1].id
            assert members == 2
            assert groups == 2
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_snapshot_survives_engine_restart(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【正常值】销毁连接池后重新连接仍读取同一数据及摘要。"""

    async def scenario() -> None:
        first = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(first)
            saved = await save_snapshot(first, "restart", snapshot)
        finally:
            await close_engine(first)
        second = database_engine(postgres_settings, test=True)
        try:
            loaded = await load_snapshot(second, "restart", saved.id)
            with pytest.raises(ValueError, match="workspace"):
                await load_snapshot(second, "other-workspace", saved.id)

            assert loaded == saved
            assert len(loaded.bars) == 1280
        finally:
            await close_engine(second)

    asyncio.run(scenario())


def test_error_version_collision_rolls_back(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【异常值】同名版本被篡改时快照和新增分类均不得提交。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            await save_snapshot(engine, "collision", snapshot)
            universe = snapshot.universe.model_copy(update={"limitations": ("changed",)})
            changed = build_snapshot(
                universe, snapshot.classifications, snapshot.bars, snapshot.source_identity, snapshot.as_of
            )

            with pytest.raises(ValueError, match="同名"):
                await save_snapshot(engine, "collision", changed)
            async with engine.connect() as connection:
                count = await connection.scalar(
                    select(func.count())
                    .select_from(MARKET_SNAPSHOTS)
                    .where(MARKET_SNAPSHOTS.c.workspace == "collision")
                )

            assert count == 1
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_history_revision_preserves_old_snapshot(postgres_settings: Settings, snapshot: Snapshot) -> None:
    """【正常值】同一历史日价格修订生成新快照，原快照保持原值。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            old = await save_snapshot(engine, "revision", snapshot)
            revised = snapshot.bars[0].model_copy(update={"close": 10.5})
            changed = build_snapshot(
                snapshot.universe,
                snapshot.classifications,
                (revised, *snapshot.bars[1:]),
                snapshot.source_identity,
                snapshot.as_of,
            )

            new = await save_snapshot(engine, "revision", changed)
            reread = await load_snapshot(engine, "revision", old.id)

            assert new.id != old.id
            assert new.content_hash != old.content_hash
            assert new.bars[0].close == 10.5
            assert reread.bars[0].close == 11
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
