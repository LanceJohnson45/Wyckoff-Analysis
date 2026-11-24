"""从授权云端只读事务增量复制，包含三个独立基准键。"""

import asyncio
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import Column, DateTime, Float, MetaData, String, Table, func, or_, select
from sqlalchemy import Date as SQLDate
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from stock_research.config import MarketSourceSettings
from stock_research.data import SOURCE_ISSUES
from stock_research.identity import SHANGHAI, digest, now
from stock_research.market_coordination import protect_market_copy
from stock_research.market_writer import checkpoint, complete, initialize_local, local_write_engine, write_batch
from stock_research.models import Bar
from stock_research.quality import BENCHMARKS
from stock_research.store import close_engine

CLOUD_METADATA = MetaData()
CLOUD_BARS = Table(
    "stock_hist_cache",
    CLOUD_METADATA,
    Column("symbol", String, primary_key=True),
    Column("adjust", String, primary_key=True),
    Column("date", SQLDate, primary_key=True),
    *(Column(name, Float) for name in ("open", "high", "low", "close", "volume")),
    Column("updated_at", DateTime(timezone=True)),
    schema="public",
)
SYNC_SCOPE = "research:us:all:benchmarks-v1"
SYNC_BATCH_ROWS = 2000
SYNC_OVERLAP = timedelta(days=1)


def source_identity(settings: MarketSourceSettings) -> str:
    """保持来源工作树已有身份算法，包括JSON空格，不能改写既有文件绑定。"""
    identity = {"host": settings.host, "port": str(settings.port), "dbname": settings.database}
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def cloud_engine(settings: MarketSourceSettings) -> AsyncEngine:
    """连接即启用只读与有界查询；数据同步不能执行云端写入。"""
    return create_async_engine(
        settings.database_url(),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        connect_args={
            "timeout": 10,
            "command_timeout": 60,
            "ssl": False if settings.sslmode == "disable" else settings.sslmode,
            "server_settings": {
                "default_transaction_read_only": "on",
                "statement_timeout": "60000",
                "idle_in_transaction_session_timeout": "120000",
            },
        },
    )


def decode_batch(
    rows: list[Mapping[str, object]], source_id: str
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """保留供应商主键；基准键与同名股票不混用，异常保留版本及日期。"""
    bars, issues = [], []
    observed = now()
    for row in rows:
        key = str(row["symbol"])
        kind = "benchmark" if key.startswith("INDEX:US:") else "stock"
        symbol = key.removeprefix("INDEX:US:") if kind == "benchmark" else key.removeprefix("US:")
        stamp = row["updated_at"]
        if not isinstance(stamp, datetime) or stamp.tzinfo is None:
            raise ValueError("云端行情修订时间缺失或不带时区，不能推进同步水位")
        version = stamp.astimezone(SHANGHAI).isoformat()
        identity = dict(market="us", kind=kind, symbol=symbol, date=row["date"], cloud_updated_at=version)
        try:
            bar = Bar(
                symbol=symbol,
                kind=kind,
                session_date=row["date"],
                **{name: row[name] for name in ("open", "high", "low", "close", "volume")},
                source=source_id,
                source_revision=stamp,
                observed_time=observed,
            )
        except ValidationError:
            issues.append({**identity, "reason": "云端OHLCV缺失或无效，待修复后重同步"})
            continue
        bars.append(
            {
                **identity,
                "adjust": "qfq",
                "imported_at": observed.isoformat(),
                **{name: getattr(bar, name) for name in ("open", "high", "low", "close", "volume")},
            }
        )
    return bars, issues


async def copy_source(
    source: AsyncEngine, local: AsyncEngine, source_id: str, since: datetime | None
) -> tuple[datetime, int, int, int]:
    """可重复读事务持有协作锁，流式传输成功后退出源事务。"""
    rows = batches = rejected = 0
    async with source.connect() as connection:
        connection = await connection.execution_options(isolation_level="REPEATABLE READ")
        async with connection.begin():
            await protect_market_copy(connection)
            lock_key = int(digest({"source": source_id, "scope": SYNC_SCOPE})[:15], 16)
            if not await connection.scalar(select(func.pg_try_advisory_xact_lock(lock_key))):
                raise ValueError("同一行情来源的研究同步任务正在运行")
            stamp = await connection.scalar(select(func.now()))
            if since is not None and since > stamp:
                raise ValueError("同步水位晚于源数据库时钟，请核对连接与时钟")
            target = or_(
                CLOUD_BARS.c.symbol.like("US:%"), CLOUD_BARS.c.symbol.in_([f"INDEX:US:{b}" for b in BENCHMARKS])
            )
            if (
                await connection.scalar(
                    select(CLOUD_BARS.c.symbol)
                    .where(
                        target,
                        CLOUD_BARS.c.adjust == "qfq",
                        CLOUD_BARS.c.updated_at.is_(None),
                    )
                    .limit(1)
                )
                is not None
            ):
                raise ValueError("云端行情存在无修订时间记录，不能推进同步水位")
            conditions = [CLOUD_BARS.c.adjust == "qfq", CLOUD_BARS.c.updated_at <= stamp, target]
            if since is not None:
                conditions.append(CLOUD_BARS.c.updated_at >= since - SYNC_OVERLAP)
            query = (
                select(CLOUD_BARS)
                .where(*conditions)
                .order_by(CLOUD_BARS.c.updated_at, CLOUD_BARS.c.symbol, CLOUD_BARS.c.date)
            )
            async with connection.stream(query) as result:
                async for batch in result.mappings().partitions(SYNC_BATCH_ROWS):
                    bars, issues = decode_batch(batch, source_id)
                    await write_batch(local, bars, issues)
                    rows += len(bars)
                    rejected += len(issues)
                    batches += 1
    return stamp, rows, batches, rejected


async def sync_market(settings: MarketSourceSettings, path: Path, *, full: bool = False) -> dict[str, object]:
    """同一云端快照分批写入本机；失败、取消或源事务失败均不推进水位。"""
    source_id = source_identity(settings)
    source = cloud_engine(settings)
    await asyncio.to_thread(path.expanduser().resolve().parent.mkdir, parents=True, exist_ok=True)
    local = local_write_engine(path)
    try:
        await initialize_local(local, source_id)
        since = None if full else await checkpoint(local, SYNC_SCOPE)
        stamp, rows, batches, rejected = await copy_source(source, local, source_id, since)
        await complete(local, SYNC_SCOPE, stamp)
        async with local.connect() as connection:
            unresolved = await connection.scalar(
                select(func.count()).select_from(SOURCE_ISSUES).where(SOURCE_ISSUES.c.market == "us")
            )
        return {
            "market": "us",
            "rows": rows,
            "batches": batches,
            "rejected": rejected,
            "issues": unresolved,
            "checkpoint": stamp.astimezone(SHANGHAI).isoformat(),
            "scope": SYNC_SCOPE,
        }
    finally:
        try:
            await close_engine(local)
        finally:
            await close_engine(source)
