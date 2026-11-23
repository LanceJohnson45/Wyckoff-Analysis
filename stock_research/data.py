"""来源分支 daily_bars 契约的只读、一致 SQLite 适配器。"""

import sqlite3
import time
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import ValidationError
from sqlalchemy import Column, Date, Float, MetaData, String, Table, create_engine, select

from stock_research.identity import digest
from stock_research.models import Bar, QualityIssue, normalize_symbol

SOURCE_METADATA = MetaData()
DAILY_BARS = Table(
    "daily_bars",
    SOURCE_METADATA,
    *(Column(name, String, primary_key=True) for name in ("market", "kind", "symbol", "adjust")),
    Column("date", Date, primary_key=True),
    *(Column(name, Float, nullable=False) for name in ("open", "high", "low", "close", "volume")),
    Column("cloud_updated_at", String, nullable=False),
    Column("imported_at", String, nullable=False),
)
SOURCE_CONFIG = Table("local_config", SOURCE_METADATA, Column("key", String, primary_key=True), Column("value", String))
SOURCE_ISSUES = Table(
    "sync_issues",
    SOURCE_METADATA,
    *(Column(name, String, primary_key=True) for name in ("market", "kind", "symbol")),
    Column("date", Date, primary_key=True),
    Column("cloud_updated_at", String),
    Column("reason", String),
)


@contextmanager
def consistent_copy(path: Path, timeout: float = 30) -> Iterator[Path]:
    """用 SQLite backup API 固定含 WAL 的一致视图；源库不创建、不写入。"""
    path = path.expanduser().resolve(strict=True)
    deadline = time.monotonic() + timeout

    def progress(status: int, remaining: int, total: int) -> None:
        if time.monotonic() > deadline:
            raise ValueError("行情一致备份超时，请检查更新进程")

    with TemporaryDirectory(prefix="stock-research-") as directory:
        copied = Path(directory) / "market.sqlite3"
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=timeout)) as source:
            with closing(sqlite3.connect(copied)) as destination:
                source.backup(destination, pages=512, progress=progress, sleep=0.01)
        yield copied


def read_market(path: Path, start: date, as_of: date) -> tuple[str, tuple[Bar, ...], tuple[QualityIssue, ...]]:
    """仅取指定窗口的前复权日线；损坏和云端隔离信息进入质量报告。"""
    with consistent_copy(path) as copied:
        engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(copied.as_uri() + "?mode=ro", uri=True))
        try:
            with engine.connect() as connection:
                source_id = connection.scalar(
                    select(SOURCE_CONFIG.c.value).where(SOURCE_CONFIG.c.key == "cloud_source")
                )
                if not source_id:
                    raise ValueError("行情库缺少 cloud_source 身份，不接受来源不明的数据")
                rows = (
                    connection.execute(
                        select(DAILY_BARS)
                        .where(
                            DAILY_BARS.c.market == "us",
                            DAILY_BARS.c.date.between(start, as_of),
                        )
                        .order_by(DAILY_BARS.c.kind, DAILY_BARS.c.symbol, DAILY_BARS.c.date)
                    )
                    .mappings()
                    .all()
                )
                rejected = (
                    connection.execute(
                        select(SOURCE_ISSUES)
                        .where(
                            SOURCE_ISSUES.c.market == "us",
                            SOURCE_ISSUES.c.date.between(start, as_of),
                        )
                        .order_by(SOURCE_ISSUES.c.kind, SOURCE_ISSUES.c.symbol, SOURCE_ISSUES.c.date)
                    )
                    .mappings()
                    .all()
                )
        finally:
            engine.dispose()
    source_identity = "local-market:" + digest(source_id)
    bars, issues = [], []
    for row in rows:
        symbol = normalize_symbol(row["symbol"])
        if row["adjust"] != "qfq":
            continue
        try:
            bars.append(
                Bar(
                    symbol=symbol,
                    kind=row["kind"],
                    session_date=row["date"],
                    **{name: row[name] for name in ("open", "high", "low", "close", "volume")},
                    source=source_identity,
                    source_revision=row["cloud_updated_at"],
                    observed_time=row["imported_at"],
                )
            )
        except ValidationError:
            issues.append(
                QualityIssue(symbol=symbol, kind=row["kind"], reason=f"invalid_ohlcv_or_revision:{row['date']}")
            )
    for row in rejected:
        issues.append(
            QualityIssue(
                symbol=normalize_symbol(row["symbol"]), kind=row["kind"], reason=f"source_quarantine:{row['date']}"
            )
        )
    return source_identity, tuple(bars), tuple(issues)
