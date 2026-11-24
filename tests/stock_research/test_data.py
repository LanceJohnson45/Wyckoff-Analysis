"""真实 SQLite 来源文件的一致读取和隔离边界。"""

import sqlite3
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import create_engine, insert, select, update

from stock_research.artifacts import data_lock, write_once
from stock_research.data import DAILY_BARS, SOURCE_ISSUES, consistent_copy, read_market

AS_OF = date(2026, 11, 20)


def test_boundary_market_row_cap_fails_instead_of_truncating(market_path: Path) -> None:
    """【边界值】有界worker输入上限与真实SQL读取配合，不能只返回部分行情。"""
    start = date(2020, 1, 1)
    _, bars, _ = read_market(market_path, start, AS_OF, max_rows=1280)
    assert len(bars) == 1280
    with pytest.raises(ValueError, match="超过"):
        read_market(market_path, start, AS_OF, max_rows=1279)
    with pytest.raises(ValueError, match="必须为正"):
        read_market(market_path, start, AS_OF, max_rows=0)


def test_normal_market_read_is_readonly_and_bounded(market_path: Path) -> None:
    """【正常值】只读适配不改变源文件，且严格按日期截断。"""
    before = market_path.read_bytes()

    source, bars, issues = read_market(market_path, AS_OF, AS_OF)

    assert source.startswith("local-market:")
    assert "synthetic" not in source
    assert len(bars) == 4
    assert {bar.session_date for bar in bars} == {AS_OF}
    assert not issues
    assert market_path.read_bytes() == before


def test_normal_backup_stays_consistent_during_source_update(market_path: Path) -> None:
    """【正常值】一致备份固定后，源库继续写入不会改变已固定视图。"""
    source = create_engine("sqlite://", creator=lambda: sqlite3.connect(market_path))
    try:
        with consistent_copy(market_path) as copied:
            with source.begin() as connection:
                connection.execute(update(DAILY_BARS).where(DAILY_BARS.c.symbol == "AAPL").values(close=10.5))
            backup = create_engine("sqlite://", creator=lambda: sqlite3.connect(copied))
            try:
                with backup.connect() as connection:
                    value = connection.scalar(select(DAILY_BARS.c.close).where(DAILY_BARS.c.symbol == "AAPL"))
            finally:
                backup.dispose()

        assert value == 11
        assert read_market(market_path, AS_OF, AS_OF)[1][-1].close == 10.5
    finally:
        source.dispose()


def test_error_invalid_bar_and_quarantine_are_reported(market_path: Path) -> None:
    """【异常值】坏OHLC与云端隔离记录保留具体日期，不静默丢K。"""
    engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(market_path))
    try:
        with engine.begin() as connection:
            connection.execute(
                update(DAILY_BARS).where(DAILY_BARS.c.symbol == "AAPL", DAILY_BARS.c.date == AS_OF).values(low=100)
            )
            connection.execute(
                insert(SOURCE_ISSUES).values(
                    market="us",
                    kind="benchmark",
                    symbol="QQQ",
                    date=AS_OF,
                    cloud_updated_at="2026-11-21T08:00:00+08:00",
                    reason="invalid source row",
                )
            )
    finally:
        engine.dispose()

    _, bars, issues = read_market(market_path, AS_OF, AS_OF)

    assert len(bars) == 3
    assert {(i.symbol, i.reason) for i in issues} == {
        ("AAPL", "invalid_ohlcv_or_revision:2026-11-20"),
        ("QQQ", "source_quarantine:2026-11-20"),
    }


def test_error_missing_source_is_not_created(tmp_path: Path) -> None:
    """【异常值】读操作不能自动创建一个空行情库。"""
    missing = tmp_path / "missing.sqlite3"

    with pytest.raises(FileNotFoundError):
        read_market(missing, AS_OF, AS_OF)

    assert not missing.exists()


def test_error_lock_conflict_and_release(tmp_path: Path) -> None:
    """【异常值】锁冲突失败，异常退出后同日锁可以再次取得。"""
    with pytest.raises(RuntimeError):
        with data_lock(tmp_path, "us", "2026-11-20"):
            with pytest.raises(ValueError, match="正在运行"):
                with data_lock(tmp_path, "us", "2026-11-20"):
                    pytest.fail("second lock must not be acquired")
            raise RuntimeError("simulated failure")

    with data_lock(tmp_path, "us", "2026-11-20"):
        assert len(list(tmp_path.glob("*.lock"))) == 1


def test_idempotent_artifact_refuses_overwrite(tmp_path: Path) -> None:
    """【幂等性】重复同内容可重用，已有内容不可覆盖。"""
    path = tmp_path / "version.json"
    write_once(path, '{"version":1}')

    write_once(path, '{"version":1}')
    with pytest.raises(ValueError, match="内容不同"):
        write_once(path, '{"version":2}')

    assert path.read_text() == '{"version":1}'
    assert list(tmp_path.iterdir()) == [path]
