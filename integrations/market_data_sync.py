"""从现有PostgreSQL日线缓存只读增量同步到本机，不向行情源发请求。"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Protocol

import psycopg
from psycopg.conninfo import conninfo_to_dict
from sqlalchemy import Column, Date, DateTime, Float, MetaData, String, Table, and_, create_engine, func, or_, select
from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from integrations.local_market_store import DailyBar, SyncIssue, LocalMarketWriter, LOCAL_TZ, local_engine, local_market_path
from integrations.postgres_base import get_postgres_dsn, postgres_enabled

SYNC_BATCH_SIZE = 2000
SYNC_OVERLAP = timedelta(days=1)
BENCHMARK_SYMBOLS = {"cn": "000300", "us": "SPY"}
CLOUD_BARS = Table(
    "stock_hist_cache", MetaData(),
    Column("symbol", String), Column("adjust", String), Column("date", Date),
    *(Column(name, Float) for name in ("open", "high", "low", "close", "volume")),
    Column("updated_at", DateTime(timezone=True)), schema="public",
)


class DeltaSource(Protocol):
    """分页传输契约，方便独立验证同步事务而不伪装云端数据库。"""

    source_id: str
    snapshot_time: datetime

    def batches(self, market: str, symbols: tuple[str, ...], since: datetime | None) -> Iterator[list[DailyBar | SyncIssue]]: ...


@dataclass(frozen=True)
class SyncResult:
    """完成一次市场或自选列表同步的统计。"""

    market: str
    rows: int
    batches: int
    checkpoint: str
    rejected: int


def sync_symbols(text: str, market: str) -> tuple[str, ...]:
    """解析可选同步列表；空字符串表示本市场全部云端股票。"""
    if market not in BENCHMARK_SYMBOLS:
        raise ValueError("仅支持 cn/us 市场")
    tokens = tuple(sorted(set(s.upper() for s in re.split(r"[\s,，;；]+", text.strip()) if s)))
    pattern = r"[0-9]{6}" if market == "cn" else r"[A-Z][A-Z0-9]{0,9}(?:[.-][A-Z0-9]{1,4})?"
    if any(not re.fullmatch(pattern, token) for token in tokens):
        raise ValueError("同步列表包含不符合本市场格式的股票代码")
    return tokens


def sync_scope(market: str, symbols: tuple[str, ...]) -> str:
    """不同自选列表分别保存水位，新加股票不会跳过其历史。"""
    return market + ":" + (sha256("\n".join(symbols).encode()).hexdigest() if symbols else "all")


class PostgresDeltaSource:
    """在同一个只读可重复读快照内按更新时间及主键翻页。"""

    def __init__(self, connection: Connection, source_id: str, snapshot_time: datetime) -> None:
        self.connection = connection
        self.source_id = source_id
        self.snapshot_time = snapshot_time

    def batches(self, market: str, symbols: tuple[str, ...], since: datetime | None) -> Iterator[list[DailyBar | SyncIssue]]:
        """查询更新行，包括历史日期的修订；不使用不稳定的OFFSET分页。"""
        benchmark_key = f"INDEX:{market.upper()}:{BENCHMARK_SYMBOLS[market]}"
        stock_filter = CLOUD_BARS.c.symbol.op("~")(r"^[0-9]{6}$") if market == "cn" else CLOUD_BARS.c.symbol.like("US:%")
        if symbols:
            keys = [f"US:{symbol}" if market == "us" else symbol for symbol in symbols]
            stock_filter = CLOUD_BARS.c.symbol.in_(keys)
        conditions = [CLOUD_BARS.c.adjust == "qfq", or_(stock_filter, CLOUD_BARS.c.symbol == benchmark_key), CLOUD_BARS.c.updated_at <= self.snapshot_time]
        if since is not None:
            conditions.append(CLOUD_BARS.c.updated_at >= since)
        query = select(CLOUD_BARS).where(and_(*conditions)).order_by(CLOUD_BARS.c.updated_at, CLOUD_BARS.c.symbol, CLOUD_BARS.c.date)
        # 服务端游标只执行一次排序，首次全市场同步也不把全表装进内存。
        result = self.connection.execution_options(stream_results=True, yield_per=SYNC_BATCH_SIZE).execute(query)
        for rows in result.mappings().partitions(SYNC_BATCH_SIZE):
            batch = []
            for row in rows:
                key = str(row["symbol"])
                kind = "benchmark" if key == benchmark_key else "stock"
                symbol = BENCHMARK_SYMBOLS[market] if kind == "benchmark" else (key.removeprefix("US:") if market == "us" else key)
                try:
                    bar = DailyBar(
                        market=market, kind=kind, symbol=symbol, day=row["date"],
                        **{name: float(row[name]) for name in ("open", "high", "low", "close", "volume")},
                        updated_at=row["updated_at"],
                    )
                    bar.validate()
                    batch.append(bar)
                except (ValueError, TypeError) as exc:
                    batch.append(SyncIssue(market, kind, symbol, row["date"], row["updated_at"], "云端OHLCV缺失或无效，待修复后重同步"))
            yield batch


@contextmanager
def postgres_delta_source() -> Iterator[PostgresDeltaSource]:
    """复用现有PG配置，强制只读并限制连接及单条查询等待时间。"""
    if not postgres_enabled():
        raise ValueError("本地同步使用现有PostgreSQL，请先配置 PG_ENABLED=1 和 PG_*；不会隐式切到其他数据库")
    dsn = get_postgres_dsn()
    try:
        config = conninfo_to_dict(dsn)
    except psycopg.Error as exc:
        raise ValueError("PG连接配置无法解析，请检查现有PG_*配置；不会显示连接串或凭证") from exc
    identity = {key: config.get(key, "") for key in ("host", "port", "dbname")}
    source_id = sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    engine = create_engine("postgresql+psycopg://", creator=lambda: psycopg.connect(
        dsn, connect_timeout=10,
        options="-c default_transaction_read_only=on -c statement_timeout=60000 -c idle_in_transaction_session_timeout=120000",
    ))
    try:
        with engine.connect().execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True) as connection, connection.begin():
            snapshot_time = connection.scalar(select(func.now()))
            yield PostgresDeltaSource(connection, source_id, snapshot_time)
    except (SQLAlchemyError, psycopg.Error) as exc:
        raise RuntimeError(f"云端数据库连接或读取失败（{type(exc).__name__}），请检查PG配置、网络和SELECT权限；本地已同步数据仍可读取") from exc
    finally:
        engine.dispose()


def sync_from_source(source: DeltaSource, market: str, *, symbols: tuple[str, ...] = (), path: Path | None = None, full: bool = False, progress: Callable[[int], None] | None = None) -> SyncResult:
    """分页提交可恢复数据，全流程成功才提交快照水位。"""
    symbols = sync_symbols(",".join(symbols), market)
    if source.snapshot_time.tzinfo is None:
        raise ValueError("云端快照时间必须带时区")
    scope = sync_scope(market, symbols)
    with local_engine(path or local_market_path(), writable=True) as engine:
        writer = LocalMarketWriter(engine, source.source_id)
        checkpoint = None if full else writer.checkpoint(scope)
        if checkpoint is not None and checkpoint > source.snapshot_time:
            raise ValueError("云端时钟早于已同步水位，请检查时钟或显式全量重同步")
        since = checkpoint - SYNC_OVERLAP if checkpoint is not None else None
        rows = batches = rejected = 0
        for batch in source.batches(market, symbols, since):
            rejected += sum(isinstance(item, SyncIssue) for item in batch)
            rows += writer.write(batch)
            batches += 1
            if progress is not None:
                progress(rows)
        writer.complete(scope, market, source.snapshot_time)
    return SyncResult(market, rows, batches, source.snapshot_time.astimezone(LOCAL_TZ).isoformat(), rejected)


def sync_market(market: str, *, symbols: tuple[str, ...] = (), path: Path | None = None, full: bool = False, progress: Callable[[int], None] | None = None) -> SyncResult:
    """显式同步入口；网页加载行情不会调用它。"""
    with postgres_delta_source() as source:
        return sync_from_source(source, market, symbols=symbols, path=path, full=full, progress=progress)
