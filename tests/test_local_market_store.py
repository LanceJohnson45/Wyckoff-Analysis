"""真实SQLite本地行情存储验证，不用于替代PostgreSQL云端测试。"""

from datetime import date, datetime, timedelta
from pathlib import Path
from collections.abc import Iterator

import pandas as pd
import pytest

from integrations.local_market_store import DailyBar, SyncIssue, local_market_summary, read_local_daily
from integrations.market_data_sync import sync_from_source, sync_scope

T0 = datetime.fromisoformat("2026-10-10T12:00:00+08:00")


def bar(symbol: str = "000001", *, close: float = 10, updated: datetime = T0, kind: str = "stock", day: date = date(2026, 10, 8)) -> DailyBar:
    """返回一根可复核日线。"""
    return DailyBar("cn", kind, symbol, day, close, close + 1, close - 1, close, 1000, updated)


class Feed:
    """内存交换数据源，只验证传输契约与实际本机事务，不模拟PG SQL。"""

    source_id = "isolated-test-source"

    def __init__(self, pages: list[list[DailyBar]], *, timestamp: datetime = T0, fail: bool = False) -> None:
        self.pages, self.snapshot_time, self.fail = pages, timestamp, fail

    def batches(self, market: str, symbols: tuple[str, ...], since: datetime | None) -> Iterator[list[DailyBar]]:
        for page in self.pages:
            yield [item for item in page if since is None or item.updated_at >= since]
        if self.fail:
            raise RuntimeError("传输中断")


def test_normal_sync_survives_reopen_and_is_idempotent(tmp_path: Path) -> None:
    """【正常值】写入后重开SQLite能读到，重复同步不产生重复K线。"""
    path = tmp_path / "market.sqlite3"
    source = Feed([[bar(), bar(day=date(2026, 10, 9))]])

    sync_from_source(source, "cn", path=path)
    sync_from_source(source, "cn", path=path)
    result = read_local_daily("000001", "cn", date(2026, 10, 9), path=path)

    assert len(result) == 2
    assert result["close"].tolist() == [10, 10]
    assert local_market_summary("cn", path=path)["symbols"] == 1
    assert local_market_summary("cn", path=path)["rows"] == 2


def test_normal_incremental_sync_propagates_historical_correction(tmp_path: Path) -> None:
    """【正常值】旧交易日价格修订按更新时间同步，不只追加新交易日。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar()]]), "cn", path=path)
    corrected = bar(close=12, updated=T0 + timedelta(days=2))

    sync_from_source(Feed([[corrected]], timestamp=T0 + timedelta(days=3)), "cn", path=path)
    result = read_local_daily("000001", "cn", date(2026, 10, 9), path=path)

    assert result["close"].tolist() == [12]
    assert len(result) == 1


def test_error_interrupted_sync_keeps_checkpoint_and_can_resume(tmp_path: Path) -> None:
    """【异常值】部分批次提交后失败，不推进水位；重试能补齐且不重复。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar()]]), "cn", path=path)
    later = T0 + timedelta(days=1)

    with pytest.raises(RuntimeError, match="传输中断"):
        sync_from_source(Feed([[bar("000002", updated=later)]], timestamp=later, fail=True), "cn", path=path)
    assert local_market_summary("cn", path=path)["last_sync"] == T0.isoformat()
    sync_from_source(Feed([[bar("000002", updated=later)], [bar("000003", updated=later)]], timestamp=later), "cn", path=path)

    assert local_market_summary("cn", path=path)["symbols"] == 3
    assert local_market_summary("cn", path=path)["last_sync"] == later.isoformat()


def test_boundary_new_symbol_scope_copies_old_history(tmp_path: Path) -> None:
    """【边界值】新自选股不会继承别的股票同步水位而漏掉历史。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar()]]), "cn", symbols=("000001",), path=path)
    old = bar("000002", updated=T0 - timedelta(days=100))

    sync_from_source(Feed([[old]], timestamp=T0 + timedelta(days=2)), "cn", symbols=("000002",), path=path)

    assert len(read_local_daily("000002", "cn", date(2026, 10, 9), path=path)) == 1
    assert sync_scope("cn", ("000001",)) != sync_scope("cn", ("000002",))


def test_boundary_stale_snapshot_cannot_overwrite_newer_bar(tmp_path: Path) -> None:
    """【边界值】另一同步范围的旧快照不能覆盖已写入的较新价格。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar(close=12, updated=T0)]], timestamp=T0), "cn", path=path)

    sync_from_source(Feed([[bar(close=10, updated=T0 - timedelta(days=1))]], timestamp=T0 - timedelta(days=1)), "cn", symbols=("000001",), path=path)

    assert read_local_daily("000001", "cn", date(2026, 10, 9), path=path)["close"].tolist() == [12]


def test_error_bad_batch_rolls_back_and_no_checkpoint(tmp_path: Path) -> None:
    """【异常值】一批中有坏价格，整批不落库也不标记同步完成。"""
    path = tmp_path / "market.sqlite3"
    invalid = DailyBar("cn", "stock", "000002", date(2026, 10, 8), 10, 9, 8, 10, 1000, T0)

    with pytest.raises(ValueError, match="OHLC"):
        sync_from_source(Feed([[bar(), invalid]]), "cn", path=path)

    assert local_market_summary("cn", path=path)["rows"] == 0
    assert local_market_summary("cn", path=path)["last_sync"] == "尚未完成同步"


def test_normal_benchmark_and_stock_same_code_stay_separate(tmp_path: Path) -> None:
    """【正常值】基准与同名股票分别存储，不能把指数当股票。"""
    path = tmp_path / "market.sqlite3"

    sync_from_source(Feed([[bar("000300", close=10), bar("000300", kind="benchmark", close=20)]]), "cn", path=path)

    assert read_local_daily("000300", "cn", date(2026, 10, 9), path=path)["close"].iloc[0] == 10
    assert read_local_daily("000300", "cn", date(2026, 10, 9), benchmark=True, path=path)["close"].iloc[0] == 20


def test_error_missing_local_store_never_creates_file(tmp_path: Path) -> None:
    """【异常值】只读网页缺库时显式失败，不生成空库或触发在线补拉。"""
    path = tmp_path / "missing.sqlite3"

    with pytest.raises(ValueError, match="先同步"):
        read_local_daily("000001", "cn", date(2026, 10, 9), path=path)

    assert not path.exists()


def test_error_different_cloud_cannot_mix_into_local_store(tmp_path: Path) -> None:
    """【异常值】不同云端数据库不能静默混入同一本地行情文件。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar()]]), "cn", path=path)
    different = Feed([[bar("000002")]])
    different.source_id = "other-database"

    with pytest.raises(ValueError, match="另一个云端"):
        sync_from_source(different, "cn", path=path)

    assert local_market_summary("cn", path=path)["symbols"] == 1


def test_normal_local_scan_works_with_network_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】真实本机库到网页读取和分析的链路在禁网环境仍可工作。"""
    from app.martin_luk_view import load_daily
    from core.martin_luk import analyze_daily

    path = tmp_path / "market.sqlite3"
    monkeypatch.setenv("LOCAL_MARKET_DB_PATH", str(path))
    dates = pd.bdate_range("2026-01-01", periods=120)
    bars = [bar(close=40 + i * 0.1, day=day.date()) for i, day in enumerate(dates)]
    sync_from_source(Feed([bars]), "cn", path=path)

    frame = load_daily("000001", "cn", dates[-1].date())
    result = analyze_daily(frame, symbol="000001", market="cn", as_of=dates[-1].date())

    assert len(frame) == 120
    assert "本地SQLite" in result.source
    assert result.data_date == str(dates[-1].date())


def test_normal_quarantine_blocks_only_affected_stock_and_recovers(tmp_path: Path) -> None:
    """【正常值】异常行不会悄悄丢K；其余股票可读，云端修正后自动解除隔离。"""
    path = tmp_path / "market.sqlite3"
    issue = SyncIssue("cn", "stock", "000002", date(2026, 10, 8), T0, "缺少成交量")

    result = sync_from_source(Feed([[bar(), issue]]), "cn", path=path)

    assert result.rejected == 1
    assert local_market_summary("cn", path=path)["issues"] == 1
    assert len(read_local_daily("000001", "cn", date(2026, 10, 9), path=path)) == 1
    with pytest.raises(ValueError, match="异常日线已隔离"):
        read_local_daily("000002", "cn", date(2026, 10, 9), path=path)
    later = T0 + timedelta(days=1)
    sync_from_source(Feed([[bar("000002", updated=later)]], timestamp=later), "cn", path=path)
    assert local_market_summary("cn", path=path)["issues"] == 0
    assert len(read_local_daily("000002", "cn", date(2026, 10, 9), path=path)) == 1


def test_boundary_older_quarantine_cannot_hide_newer_correction(tmp_path: Path) -> None:
    """【边界值】旧范围的异常快照不能让已修正股票重新被隔离。"""
    path = tmp_path / "market.sqlite3"
    sync_from_source(Feed([[bar(close=12)]]), "cn", path=path)
    old = T0 - timedelta(days=1)
    issue = SyncIssue("cn", "stock", "000001", date(2026, 10, 8), old, "旧异常")

    sync_from_source(Feed([[issue]], timestamp=old), "cn", symbols=("000001",), path=path)

    assert local_market_summary("cn", path=path)["issues"] == 0
    assert read_local_daily("000001", "cn", date(2026, 10, 9), path=path)["close"].iloc[0] == 12
