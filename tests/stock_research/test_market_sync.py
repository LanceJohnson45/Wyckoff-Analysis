"""真实临时PostgreSQL只读源到SQLite的增量同步验证。"""

import asyncio
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.exc import DBAPIError

from stock_research.config import MarketSourceSettings, Settings
from stock_research.data import DAILY_BARS, SOURCE_ISSUES
from stock_research.identity import SHANGHAI
from stock_research.market_sync import (
    CLOUD_BARS,
    CLOUD_METADATA,
    SYNC_SCOPE,
    cloud_engine,
    source_identity,
    sync_market,
)
from stock_research.market_writer import checkpoint, complete, initialize_local, local_write_engine
from stock_research.store import close_engine, database_engine

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
STAMP = datetime(2026, 11, 21, 8, tzinfo=SHANGHAI)
SESSION = date(2026, 11, 20)


def source_settings(settings: Settings) -> MarketSourceSettings:
    """只连接临时_test库，密码是任意合成值，服务端Unix socket认证为trust。"""
    return MarketSourceSettings(
        _env_file=None,
        enabled=True,
        host=settings.postgres_host,
        database=settings.database_url(test=True).database,
        user=settings.postgres_user,
        password="synthetic-test",
        sslmode="disable",
    )


async def seed_source(settings: Settings, *, invalid: bool = False) -> None:
    """仅测试预置过程使用写连接，生产同步保持只读。"""
    engine = database_engine(settings, test=True)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(CLOUD_METADATA.create_all)
            await connection.execute(delete(CLOUD_BARS))
            await connection.execute(
                insert(CLOUD_BARS),
                [
                    dict(
                        symbol=symbol,
                        adjust="qfq",
                        date=SESSION,
                        open=10,
                        high=12,
                        low=9,
                        close=None if invalid and symbol == "US:AAPL" else 11,
                        volume=1000,
                        updated_at=STAMP,
                    )
                    for symbol in ("US:AAPL", "US:QQQ", "INDEX:US:SPY", "INDEX:US:QQQ", "INDEX:US:IWM")
                ],
            )
    finally:
        await close_engine(engine)


def test_normal_sync_preserves_three_benchmarks_and_stock_identity(postgres_settings: Settings, tmp_path: Path) -> None:
    """【正常值】三个基准及同名QQQ股票分别写入，水位只用新范围。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        path = tmp_path / "market.sqlite3"

        result = await sync_market(source_settings(postgres_settings), path)
        local = local_write_engine(path)
        try:
            async with local.connect() as connection:
                rows = (await connection.execute(select(DAILY_BARS.c.kind, DAILY_BARS.c.symbol))).all()
            stamp = await checkpoint(local, SYNC_SCOPE)

            assert result["rows"] == 5
            assert result["issues"] == 0
            assert set(rows) == {
                ("stock", "AAPL"),
                ("stock", "QQQ"),
                ("benchmark", "SPY"),
                ("benchmark", "QQQ"),
                ("benchmark", "IWM"),
            }
            assert stamp is not None and stamp > STAMP
            assert await checkpoint(local, "us:all") is None
        finally:
            await close_engine(local)

    asyncio.run(scenario())


def test_normal_large_sync_uses_bounded_large_batches(postgres_settings: Settings, tmp_path: Path) -> None:
    """【正常值】历史窗口不拆成大量远端往返和SQLite事务，所有行仍独立读回。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        admin = database_engine(postgres_settings, test=True)
        try:
            async with admin.begin() as connection:
                await connection.execute(delete(CLOUD_BARS))
                await connection.execute(
                    insert(CLOUD_BARS),
                    [
                        dict(
                            symbol=f"US:T{symbol}",
                            adjust="qfq",
                            date=SESSION - timedelta(days=day),
                            open=10,
                            high=12,
                            low=9,
                            close=11,
                            volume=1000,
                            updated_at=STAMP,
                        )
                        for symbol in range(20)
                        for day in range(600)
                    ],
                )
            path = tmp_path / "large-sync.sqlite3"

            result = await sync_market(source_settings(postgres_settings), path)
            local = local_write_engine(path)
            try:
                async with local.connect() as connection:
                    count = await connection.scalar(select(func.count()).select_from(DAILY_BARS))

                assert result["rows"] == count == 12000
                assert result["batches"] <= 2
            finally:
                await close_engine(local)
        finally:
            await close_engine(admin)

    asyncio.run(scenario())


def test_error_cloud_connection_rejects_writes(postgres_settings: Settings) -> None:
    """【异常值】生产源连接的事务只读真实生效，INSERT被服务器拒绝。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        engine = cloud_engine(source_settings(postgres_settings))
        try:
            async with engine.begin() as connection:
                readonly = await connection.scalar(select(func.current_setting("transaction_read_only")))
                with pytest.raises(DBAPIError):
                    await connection.execute(delete(CLOUD_BARS))

            assert readonly == "on"
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_incremental_sync_includes_historical_revision(postgres_settings: Settings, tmp_path: Path) -> None:
    """【正常值】历史日期未变但更新时间变新时，增量同步取得修订。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        source = source_settings(postgres_settings)
        path = tmp_path / "incremental.sqlite3"
        await sync_market(source, path)
        admin = database_engine(postgres_settings, test=True)
        try:
            async with admin.begin() as connection:
                await connection.execute(
                    update(CLOUD_BARS).where(CLOUD_BARS.c.symbol == "US:AAPL").values(close=10.7, updated_at=func.now())
                )

            result = await sync_market(source, path)
        finally:
            await close_engine(admin)
        local = local_write_engine(path)
        try:
            async with local.connect() as connection:
                value = await connection.scalar(select(DAILY_BARS.c.close).where(DAILY_BARS.c.symbol == "AAPL"))

            assert result["rows"] == 1
            assert value == 10.7
        finally:
            await close_engine(local)

    asyncio.run(scenario())


@pytest.mark.parametrize("revision_delta,close,changed", [(0, 11, False), (0, 10.5, True), (1, 11, True)])
def test_normal_overlap_only_reimports_changed_source_rows(
    postgres_settings: Settings, tmp_path: Path, revision_delta: int, close: float, changed: bool
) -> None:
    """【正常值】重叠重放不改未变行的导入时间，同版本修正和新版本仍入库。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        source = source_settings(postgres_settings)
        path = tmp_path / "overlap.sqlite3"
        await sync_market(source, path, full=True)
        local = local_write_engine(path)
        admin = database_engine(postgres_settings, test=True)
        try:
            async with local.connect() as connection:
                before = await connection.scalar(select(DAILY_BARS.c.imported_at).where(DAILY_BARS.c.symbol == "AAPL"))
            async with admin.begin() as connection:
                await connection.execute(
                    update(CLOUD_BARS)
                    .where(CLOUD_BARS.c.symbol == "US:AAPL")
                    .values(close=close, updated_at=STAMP + timedelta(hours=revision_delta))
                )

            result = await sync_market(source, path, full=True)
            async with local.connect() as connection:
                after = (
                    await connection.execute(
                        select(DAILY_BARS.c.imported_at, DAILY_BARS.c.close).where(DAILY_BARS.c.symbol == "AAPL")
                    )
                ).one()

            assert result["rows"] == 5
            assert after.close == close
            assert (after.imported_at != before) is changed
        finally:
            await close_engine(local)
            await close_engine(admin)

    asyncio.run(scenario())


def test_normal_revision_repairs_quarantine(postgres_settings: Settings, tmp_path: Path) -> None:
    """【正常值】新修订修复隔离，随后旧修订不能覆盖新行情。"""

    async def scenario() -> None:
        await seed_source(postgres_settings, invalid=True)
        source = source_settings(postgres_settings)
        path = tmp_path / "market.sqlite3"
        first = await sync_market(source, path, full=True)
        admin = database_engine(postgres_settings, test=True)
        try:
            async with admin.begin() as connection:
                await connection.execute(
                    update(CLOUD_BARS)
                    .where(CLOUD_BARS.c.symbol == "US:AAPL")
                    .values(close=10.5, updated_at=STAMP + timedelta(hours=1))
                )
            repaired = await sync_market(source, path, full=True)
            async with admin.begin() as connection:
                await connection.execute(
                    update(CLOUD_BARS).where(CLOUD_BARS.c.symbol == "US:AAPL").values(close=11.5, updated_at=STAMP)
                )
            stale = await sync_market(source, path, full=True)
        finally:
            await close_engine(admin)
        local = local_write_engine(path)
        try:
            async with local.connect() as connection:
                value = await connection.scalar(select(DAILY_BARS.c.close).where(DAILY_BARS.c.symbol == "AAPL"))
                remaining = await connection.scalar(select(func.count()).select_from(SOURCE_ISSUES))

            assert first["rejected"] == first["issues"] == 1
            assert repaired["issues"] == stale["issues"] == remaining == 0
            assert value == 10.5
        finally:
            await close_engine(local)

    asyncio.run(scenario())


def test_error_wrong_source_never_advances_checkpoint(postgres_settings: Settings, tmp_path: Path) -> None:
    """【异常值】本机文件已绑定其他来源时拒绝同步，既有水位保持原状。"""

    async def scenario() -> None:
        path = tmp_path / "other.sqlite3"
        local = local_write_engine(path)
        try:
            await initialize_local(local, "different-source")
            await complete(local, SYNC_SCOPE, STAMP)

            with pytest.raises(ValueError, match="其他云端"):
                await sync_market(source_settings(postgres_settings), path)

            assert await checkpoint(local, SYNC_SCOPE) == STAMP
        finally:
            await close_engine(local)

    asyncio.run(scenario())


def test_error_advisory_lock_prevents_competing_sync(postgres_settings: Settings, tmp_path: Path) -> None:
    """【异常值】同来源协作锁冲突时不写入水位，释放后可继续。"""
    from stock_research.identity import digest

    async def scenario() -> None:
        await seed_source(postgres_settings)
        settings = source_settings(postgres_settings)
        identity = source_identity(settings)
        key = int(digest({"source": identity, "scope": SYNC_SCOPE})[:15], 16)
        engine = cloud_engine(settings)
        path = tmp_path / "locked.sqlite3"
        try:
            async with engine.begin() as connection:
                await connection.scalar(select(func.pg_advisory_xact_lock(key)))

                with pytest.raises(ValueError, match="正在运行"):
                    await sync_market(settings, path)

            result = await sync_market(settings, path)
            assert result["rows"] == 5
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_missing_revision_preserves_checkpoint(postgres_settings: Settings, tmp_path: Path) -> None:
    """【异常值】未知修订时间不能被SQL比较静默过滤，既有水位不变。"""

    async def scenario() -> None:
        await seed_source(postgres_settings)
        source = source_settings(postgres_settings)
        path = tmp_path / "missing-revision.sqlite3"
        local = local_write_engine(path)
        admin = database_engine(postgres_settings, test=True)
        try:
            await initialize_local(local, source_identity(source))
            await complete(local, SYNC_SCOPE, STAMP)
            async with admin.begin() as connection:
                await connection.execute(
                    update(CLOUD_BARS).where(CLOUD_BARS.c.symbol == "US:AAPL").values(updated_at=None)
                )

            with pytest.raises(ValueError, match="无修订时间"):
                await sync_market(source, path)

            assert await checkpoint(local, SYNC_SCOPE) == STAMP
        finally:
            await close_engine(local)
            await close_engine(admin)

    asyncio.run(scenario())


def test_error_cancelled_batch_preserves_partial_rows_without_checkpoint(
    postgres_settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """【异常值】已提交批次保留，后续取消不能提交水位，重跑可补齐。"""
    import stock_research.market_sync as module

    original = module.decode_batch
    calls = 0

    def cancelled_decode(rows: list, source_id: str) -> tuple[list, list]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise asyncio.CancelledError
        return original(rows, source_id)

    async def scenario() -> None:
        await seed_source(postgres_settings)
        source = source_settings(postgres_settings)
        path = tmp_path / "cancelled.sqlite3"
        monkeypatch.setattr(module, "SYNC_BATCH_ROWS", 2)
        monkeypatch.setattr(module, "decode_batch", cancelled_decode)

        with pytest.raises(asyncio.CancelledError):
            await sync_market(source, path)
        local = local_write_engine(path)
        try:
            async with local.connect() as connection:
                partial = await connection.scalar(select(func.count()).select_from(DAILY_BARS))
            saved = await checkpoint(local, SYNC_SCOPE)
            monkeypatch.setattr(module, "decode_batch", original)
            result = await sync_market(source, path)

            assert partial == 2
            assert saved is None
            assert result["rows"] == 5
            assert await checkpoint(local, SYNC_SCOPE) is not None
        finally:
            await close_engine(local)

    asyncio.run(scenario())
