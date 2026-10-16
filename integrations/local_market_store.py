"""本地SQLite日线库。网页只读；显式同步才写入，不接触在线行情接口。"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
import math
import os
from pathlib import Path
import sqlite3
from typing import Iterator, Literal
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import Column, Date, Float, MetaData, String, Table, URL, create_engine, delete, distinct, func, select, tuple_
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_METADATA = MetaData()
LOCAL_BARS = Table(
    "daily_bars", LOCAL_METADATA,
    Column("market", String, primary_key=True), Column("kind", String, primary_key=True),
    Column("symbol", String, primary_key=True), Column("adjust", String, primary_key=True),
    Column("date", Date, primary_key=True),
    *(Column(name, Float, nullable=False) for name in ("open", "high", "low", "close", "volume")),
    Column("cloud_updated_at", String, nullable=False), Column("imported_at", String, nullable=False),
)
SYNC_STATE = Table(
    "sync_state", LOCAL_METADATA,
    Column("scope", String, primary_key=True), Column("market", String, nullable=False),
    Column("checkpoint", String, nullable=False),
)
LOCAL_CONFIG = Table("local_config", LOCAL_METADATA, Column("key", String, primary_key=True), Column("value", String, nullable=False))
SYNC_ISSUES = Table(
    "sync_issues", LOCAL_METADATA,
    Column("market", String, primary_key=True), Column("kind", String, primary_key=True),
    Column("symbol", String, primary_key=True), Column("date", Date, primary_key=True),
    Column("cloud_updated_at", String, nullable=False), Column("reason", String, nullable=False),
)
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class DailyBar:
    """云端与本机之间唯一的日线交换格式。"""

    market: Literal["cn", "us"]
    kind: Literal["stock", "benchmark"]
    symbol: str
    day: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    updated_at: datetime

    def validate(self) -> None:
        """拒绝不合理行情，保留指数和停牌历史可能出现的零量。"""
        prices = (self.open, self.high, self.low, self.close)
        if self.market not in {"cn", "us"} or self.kind not in {"stock", "benchmark"} or not self.symbol:
            raise ValueError("日线市场、种类或代码无效")
        if not all(math.isfinite(v) and v > 0 for v in prices):
            raise ValueError(f"{self.symbol} {self.day} 的价格缺失或无效")
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError(f"{self.symbol} {self.day} 的OHLC关系不合理")
        if not math.isfinite(self.volume) or self.volume < 0:
            raise ValueError(f"{self.symbol} {self.day} 的成交量无效")
        if self.updated_at.tzinfo is None:
            raise ValueError("云端更新时间必须带时区")


@dataclass(frozen=True)
class SyncIssue:
    """云端异常日线的隔离记录；保留日期，防止无声丢K后继续分析。"""

    market: str
    kind: str
    symbol: str
    day: date
    updated_at: datetime
    reason: str


def local_market_path() -> Path:
    """默认保存到仓库data目录，支持显式本机路径。"""
    configured = os.getenv("LOCAL_MARKET_DB_PATH", "").strip()
    path = Path(configured).expanduser() if configured else PROJECT_ROOT / "data" / "local_market.sqlite3"
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


@contextmanager
def local_engine(path: Path, *, writable: bool = False) -> Iterator[Engine]:
    """读操作不创建文件；写操作显式建表，连接随操作关闭。"""
    if writable:
        path.parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(URL.create("sqlite", database=str(path)), connect_args={"timeout": 30})
    else:
        if not path.is_file():
            raise ValueError("本地行情库尚未建立，请先同步云端数据")
        engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=30))
    try:
        if writable:
            LOCAL_METADATA.create_all(engine)
        yield engine
    except (SQLAlchemyError, sqlite3.Error) as exc:
        raise RuntimeError("本地行情库操作失败，请检查文件格式、写入权限、磁盘空间或占用情况") from exc
    finally:
        engine.dispose()


class LocalMarketWriter:
    """一个同步过程使用的本地写入器，不保存云端凭证。"""

    def __init__(self, engine: Engine, source_id: str) -> None:
        self.engine = engine
        with engine.begin() as connection:
            connection.execute(insert(LOCAL_CONFIG).values(key="cloud_source", value=source_id).on_conflict_do_nothing())
            current = connection.scalar(select(LOCAL_CONFIG.c.value).where(LOCAL_CONFIG.c.key == "cloud_source"))
            if current != source_id:
                raise ValueError("本地库属于另一个云端数据库，请为新数据源设置独立的 LOCAL_MARKET_DB_PATH")

    def checkpoint(self, scope: str) -> datetime | None:
        """获取成功完成的同步水位，不把部分写入当作完成。"""
        with self.engine.connect() as connection:
            raw = connection.scalar(select(SYNC_STATE.c.checkpoint).where(SYNC_STATE.c.scope == scope))
        return datetime.fromisoformat(raw) if raw else None

    def write(self, bars: list[DailyBar | SyncIssue]) -> int:
        """每批原子更新；旧快照不能覆盖更晚的云端修订。"""
        imported_at = datetime.now(LOCAL_TZ).isoformat()
        payload = []
        issues = []
        for bar in bars:
            if isinstance(bar, SyncIssue):
                issues.append({"market": bar.market, "kind": bar.kind, "symbol": bar.symbol, "date": bar.day,
                               "cloud_updated_at": bar.updated_at.astimezone(LOCAL_TZ).isoformat(), "reason": bar.reason})
                continue
            bar.validate()
            payload.append({
                "market": bar.market, "kind": bar.kind, "symbol": bar.symbol, "adjust": "qfq", "date": bar.day,
                "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close, "volume": bar.volume,
                "cloud_updated_at": bar.updated_at.astimezone(LOCAL_TZ).isoformat(), "imported_at": imported_at,
            })
        if not payload and not issues:
            return 0
        statement = insert(LOCAL_BARS)
        upsert = statement.on_conflict_do_update(
            index_elements=["market", "kind", "symbol", "adjust", "date"],
            set_={name: statement.excluded[name] for name in ("open", "high", "low", "close", "volume", "cloud_updated_at", "imported_at")},
            where=statement.excluded.cloud_updated_at >= LOCAL_BARS.c.cloud_updated_at,
        )
        with self.engine.begin() as connection:
            if payload:
                connection.execute(upsert, payload)
                versions = {(item["market"], item["kind"], item["symbol"], item["date"]): item["cloud_updated_at"] for item in payload}
                previous_issues = connection.execute(select(SYNC_ISSUES).where(
                    SYNC_ISSUES.c.market.in_({item["market"] for item in payload}),
                    SYNC_ISSUES.c.symbol.in_({item["symbol"] for item in payload}),
                )).mappings().all()
                resolved = []
                for issue in previous_issues:
                    key = (issue["market"], issue["kind"], issue["symbol"], issue["date"])
                    if key in versions and versions[key] >= issue["cloud_updated_at"]:
                        resolved.append(key)
                if resolved:
                    connection.execute(delete(SYNC_ISSUES).where(tuple_(SYNC_ISSUES.c.market, SYNC_ISSUES.c.kind, SYNC_ISSUES.c.symbol, SYNC_ISSUES.c.date).in_(resolved)))
            if issues:
                existing = connection.execute(select(LOCAL_BARS).where(
                    LOCAL_BARS.c.market.in_({item["market"] for item in issues}),
                    LOCAL_BARS.c.symbol.in_({item["symbol"] for item in issues}),
                    LOCAL_BARS.c.date.in_({item["date"] for item in issues}),
                )).mappings().all()
                known = {(item["market"], item["kind"], item["symbol"], item["date"]): item["cloud_updated_at"] for item in existing}
                issues = [item for item in issues if known.get((item["market"], item["kind"], item["symbol"], item["date"]), "") <= item["cloud_updated_at"]]
            if issues:
                issue_insert = insert(SYNC_ISSUES)
                connection.execute(issue_insert.on_conflict_do_update(
                    index_elements=["market", "kind", "symbol", "date"],
                    set_={"cloud_updated_at": issue_insert.excluded.cloud_updated_at, "reason": issue_insert.excluded.reason},
                    where=issue_insert.excluded.cloud_updated_at >= SYNC_ISSUES.c.cloud_updated_at,
                ), issues)
        return len(payload)

    def complete(self, scope: str, market: str, timestamp: datetime) -> None:
        """仅在全部批次写入成功后推进水位，避免并发旧任务倒退水位。"""
        statement = insert(SYNC_STATE).values(scope=scope, market=market, checkpoint=timestamp.astimezone(LOCAL_TZ).isoformat())
        with self.engine.begin() as connection:
            connection.execute(statement.on_conflict_do_update(
                index_elements=["scope"], set_={"checkpoint": statement.excluded.checkpoint},
                where=statement.excluded.checkpoint > SYNC_STATE.c.checkpoint,
            ))


def read_local_daily(symbol: str, market: str, as_of: date, *, benchmark: bool = False, path: Path | None = None) -> pd.DataFrame:
    """读取截止日内全部本地历史，不隐式同步、不在线补拉。"""
    path = path or local_market_path()
    try:
        with local_engine(path) as engine, engine.connect() as connection:
            issues = connection.scalar(select(func.count()).select_from(SYNC_ISSUES).where(
                SYNC_ISSUES.c.market == market, SYNC_ISSUES.c.kind == ("benchmark" if benchmark else "stock"),
                SYNC_ISSUES.c.symbol == symbol, SYNC_ISSUES.c.date <= as_of,
            ))
            if issues:
                raise ValueError(f"{symbol} 有 {issues} 根云端异常日线已隔离，请修复云端数据后重同步；暂不生成分析结果")
            rows = connection.execute(select(LOCAL_BARS).where(
                LOCAL_BARS.c.market == market, LOCAL_BARS.c.kind == ("benchmark" if benchmark else "stock"),
                LOCAL_BARS.c.symbol == symbol, LOCAL_BARS.c.adjust == "qfq", LOCAL_BARS.c.date <= as_of,
            ).order_by(LOCAL_BARS.c.date)).mappings().all()
    except SQLAlchemyError as exc:
        raise ValueError("本地行情库无法读取，请检查路径与文件后重新同步") from exc
    if not rows:
        raise ValueError(f"本地没有 {symbol} 截至 {as_of} 的日线，请先同步；网页不会自动在线补拉")
    frame = pd.DataFrame(rows)
    frame["date"] = pd.to_datetime(frame["date"])
    frame.attrs["source"] = "本地SQLite（云端日线同步）"
    frame.attrs["last_synced_at"] = frame["imported_at"].max()
    return frame[["date", "open", "high", "low", "close", "volume"]].copy()


def local_market_summary(market: str, *, path: Path | None = None) -> dict[str, str | int]:
    """读取市场覆盖和最近成功同步时间，不触发网络或创建数据库。"""
    path = path or local_market_path()
    if not path.is_file():
        return {"symbols": 0, "rows": 0, "latest_date": "尚无数据", "last_sync": "尚未同步", "issues": 0}
    with local_engine(path) as engine, engine.connect() as connection:
        row = connection.execute(select(
            func.count(distinct(LOCAL_BARS.c.symbol)).label("symbols"), func.count().label("rows"),
            func.max(LOCAL_BARS.c.date).label("latest_date"),
        ).where(LOCAL_BARS.c.market == market, LOCAL_BARS.c.kind == "stock")).mappings().one()
        synced = connection.scalar(select(func.max(SYNC_STATE.c.checkpoint)).where(SYNC_STATE.c.market == market))
        issues = connection.scalar(select(func.count()).select_from(SYNC_ISSUES).where(SYNC_ISSUES.c.market == market))
    return {"symbols": int(row["symbols"]), "rows": int(row["rows"]), "latest_date": str(row["latest_date"] or "尚无数据"), "last_sync": str(synced or "尚未完成同步"), "issues": int(issues)}


def read_local_issues(market: str, *, path: Path | None = None) -> pd.DataFrame:
    """展示有限数量的隔离明细，便于定位云端需要修复的日期。"""
    with local_engine(path or local_market_path()) as engine, engine.connect() as connection:
        rows = connection.execute(select(SYNC_ISSUES.c.symbol, SYNC_ISSUES.c.date, SYNC_ISSUES.c.reason).where(
            SYNC_ISSUES.c.market == market,
        ).order_by(SYNC_ISSUES.c.symbol, SYNC_ISSUES.c.date).limit(200)).mappings().all()
    return pd.DataFrame(rows).rename(columns={"symbol": "代码", "date": "日期", "reason": "隔离原因"})
