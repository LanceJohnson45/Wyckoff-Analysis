"""研究数据固定样本与真实临时 PostgreSQL；不访问外部服务。"""

import math
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, insert

from stock_research.calendar import sessions_ending
from stock_research.config import Settings
from stock_research.data import DAILY_BARS, SOURCE_CONFIG, SOURCE_METADATA
from stock_research.identity import SHANGHAI
from stock_research.models import Bar, Classification, ClassificationVersion, Snapshot, UniverseMember, UniverseVersion
from stock_research.quality import BENCHMARKS, build_snapshot

AS_OF = date(2026, 11, 20)
STAMP = datetime(2026, 11, 21, 8, tzinfo=SHANGHAI)
SOCKET_CONNECT = socket.socket.connect


@pytest.fixture(autouse=True)
def isolated_research_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """其他套件可加载根.env；合成研究输入不能意外使用真实路径或身份。"""
    for key in tuple(os.environ):
        if key.startswith(("STOCK_RESEARCH_", "PG_")):
            monkeypatch.delenv(key)


@pytest.fixture
def local_postgres_socket(monkeypatch: pytest.MonkeyPatch, postgres_settings: Settings, _no_network: None) -> None:
    """只豁免本 fixture 临时簇的 Unix socket，TCP 和其他 socket 仍被禁止。"""
    allowed = str(Path(postgres_settings.postgres_host) / ".s.PGSQL.5432")

    def connect(client: socket.socket, address: object) -> object:
        if client.family != socket.AF_UNIX or address != allowed:
            raise RuntimeError("Tests must not make real network calls")
        return SOCKET_CONNECT(client, address)

    monkeypatch.setattr(socket.socket, "connect", connect)


@pytest.fixture(scope="session")
def universe() -> UniverseVersion:
    """单股双指数归属的显式合成样本，不能用于正式研究。"""
    return UniverseVersion(
        version="fixture-v1",
        create_time=STAMP,
        members=tuple(
            UniverseMember(
                symbol="AAPL",
                name="Apple",
                index=index,
                source="synthetic-fixture",
                effective_date=date(2020, 1, 1),
                obtained_time=STAMP,
            )
            for index in ("sp500", "nasdaq100")
        ),
        limitations=("synthetic-fixture",),
    )


@pytest.fixture(scope="session")
def classifications() -> ClassificationVersion:
    """行业及显式已确认主题的固定样本。"""
    return ClassificationVersion(
        version="fixture-v1",
        records=(
            Classification(
                symbol="AAPL",
                company_name="Apple",
                industry="Hardware",
                themes=("Devices",),
                source="synthetic-fixture",
                obtained_time=STAMP,
                effective_date=date(2020, 1, 1),
            ),
        ),
    )


@pytest.fixture(scope="session")
def bars() -> tuple[Bar, ...]:
    """完整320交易日股票及三个基准，操作时间故意晚于行情日。"""
    return tuple(
        Bar(
            symbol=symbol,
            kind=kind,
            session_date=day,
            open=10,
            high=12,
            low=9,
            close=11,
            volume=1000,
            source="synthetic-fixture",
            source_revision=STAMP,
            observed_time=STAMP,
        )
        for kind, symbol in (("stock", "AAPL"), *(("benchmark", name) for name in BENCHMARKS))
        for day in sessions_ending(AS_OF, 320)
    )


def trending_sample() -> tuple[Bar, ...]:
    """固定输入行情，用于旧模块黄金值与新公式对拍，不复制指标计算。"""
    days = sessions_ending(AS_OF, 120)
    result = []
    for index, day in enumerate(days):
        close = 100 + index * 0.2 + math.sin(index / 5)
        if index >= 117:
            close -= (index - 116) * 0.35
        width = 0.35 if index >= 117 else 1.0
        result.append(
            Bar(
                symbol="AAPL",
                kind="stock",
                session_date=day,
                open=close - 0.1,
                high=close + width,
                low=close - width,
                close=close,
                volume=6000 if index >= 117 else 10000 + index * 10,
                source="synthetic-golden",
                source_revision=STAMP,
                observed_time=STAMP,
            )
        )
    return tuple(result)


@pytest.fixture
def trending_bars() -> tuple[Bar, ...]:
    """已固定定义的120日合成行情。"""
    return trending_sample()


@pytest.fixture
def snapshot(universe: UniverseVersion, classifications: ClassificationVersion, bars: tuple[Bar, ...]) -> Snapshot:
    """走真实快照构建路径的测试输入。"""
    return build_snapshot(universe, classifications, bars, "synthetic-fixture", AS_OF)


@pytest.fixture
def spring_snapshot(snapshot: Snapshot) -> Snapshot:
    """独立合成输入：320日横盘后下探并收复，不能用于真实策略收益结论。"""
    bars = []
    stock = [bar for bar in snapshot.bars if bar.kind == "stock"]
    for index, bar in enumerate(stock):
        values = dict(open=10.0, high=10.1, low=9.9, close=10.0, volume=1000.0)
        if index == len(stock) - 2:
            values.update(open=10.1, high=10.2, low=9.7, close=10.05)
        if index == len(stock) - 1:
            values.update(open=9.9, high=10.6, low=9.6, close=10.5, volume=2000.0)
        bars.append(bar.model_copy(update=values))
    bars.extend(bar for bar in snapshot.bars if bar.kind == "benchmark")
    return build_snapshot(snapshot.universe, snapshot.classifications, tuple(bars), "synthetic-spring", snapshot.as_of)


@pytest.fixture
def market_path(tmp_path: Path, bars: tuple[Bar, ...]) -> Path:
    """只为生产 SQLite 行情源造真实文件；研究状态仍使用 PostgreSQL。"""
    path = tmp_path / "source.sqlite3"
    engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(path))
    try:
        SOURCE_METADATA.create_all(engine)
        with engine.begin() as connection:
            connection.execute(insert(SOURCE_CONFIG).values(key="cloud_source", value="synthetic-fixture"))
            connection.execute(
                insert(DAILY_BARS),
                [
                    dict(
                        market="us",
                        kind=b.kind,
                        symbol=b.symbol,
                        adjust=b.adjustment,
                        date=b.session_date,
                        open=b.open,
                        high=b.high,
                        low=b.low,
                        close=b.close,
                        volume=b.volume,
                        cloud_updated_at=b.source_revision.isoformat(),
                        imported_at=b.observed_time.isoformat(),
                    )
                    for b in bars
                ],
            )
    finally:
        engine.dispose()
    return path


@pytest.fixture(scope="session")
def postgres_settings() -> Iterator[Settings]:
    """独立临时簇、仅 Unix socket、仅 _test 库；退出停止并清理。"""
    binaries = {name: shutil.which(name) for name in ("initdb", "pg_ctl", "createdb")}
    if not all(binaries.values()):
        pytest.skip("本机缺 PostgreSQL 服务端工具；需安装 initdb/pg_ctl/createdb 才能执行真实集成测试")
    with tempfile.TemporaryDirectory(prefix="srpg-", dir="/tmp") as directory:
        cluster = str(Path(directory) / "data")
        subprocess.run(
            [binaries["initdb"], "-D", cluster, "-U", "research_test", "-A", "trust", "--no-locale", "-E", "UTF8"],
            check=True,
            capture_output=True,
            timeout=30,
        )
        subprocess.run(
            [
                binaries["pg_ctl"],
                "-D",
                cluster,
                "-l",
                str(Path(directory) / "postgres.log"),
                "-o",
                f"-k {directory} -h '' -p 5432 -F",
                "-w",
                "-t",
                "10",
                "start",
            ],
            check=True,
            capture_output=True,
            timeout=20,
        )
        try:
            subprocess.run(
                [binaries["createdb"], "-h", directory, "-U", "research_test", "stock_research_test"],
                check=True,
                capture_output=True,
                timeout=10,
            )
            # Session fixtures先于function级环境隔离执行，不能继承其他套件加载的.env。
            environment = {key: value for key, value in os.environ.items() if not key.startswith("STOCK_RESEARCH_")}
            with patch.dict(os.environ, environment, clear=True):
                settings = Settings(
                    _env_file=None,
                    postgres_host=directory,
                    postgres_database="stock_research",
                    postgres_user="research_test",
                    postgres_password="",
                )
            yield settings
        finally:
            subprocess.run(
                [binaries["pg_ctl"], "-D", cluster, "-m", "immediate", "-w", "stop"],
                check=True,
                capture_output=True,
                timeout=15,
            )
