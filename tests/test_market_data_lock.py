"""Native Unix-socket PostgreSQL proofs; no cloud database or provider I/O."""

import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from integrations.market_data_lock import (
    MARKET_DATA_KEY,
    US_MARKET_JOB_KEY,
    MarketDataBusy,
    cli_market_job,
    us_market_job,
)
from integrations.postgres_base import connect_postgres, upsert_rows


@pytest.fixture(autouse=True)
def offline_provider(monkeypatch):
    from curl_cffi.requests import Session

    def denied(*args, **kwargs):
        raise AssertionError("Market coordination tests must not access an external provider")

    monkeypatch.setattr(Session, "request", denied)


@pytest.fixture(scope="module")
def market_postgres():
    binaries = {name: shutil.which(name) for name in ("initdb", "pg_ctl", "createdb")}
    if not all(binaries.values()):
        pytest.skip("Install PostgreSQL server tools for native market-coordination integration tests")
    with tempfile.TemporaryDirectory(prefix="mkpg-", dir="/tmp") as directory:
        cluster = str(Path(directory) / "data")
        subprocess.run(
            [
                binaries["initdb"],
                "-D",
                cluster,
                "-U",
                "market_test",
                "-A",
                "trust",
                "--no-locale",
                "-E",
                "UTF8",
            ],
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
                [
                    binaries["createdb"],
                    "-h",
                    directory,
                    "-U",
                    "market_test",
                    "market_source_test",
                ],
                check=True,
                capture_output=True,
                timeout=10,
            )
            yield f"host={directory} dbname=market_source_test user=market_test sslmode=disable connect_timeout=5"
        finally:
            subprocess.run(
                [binaries["pg_ctl"], "-D", cluster, "-m", "immediate", "-w", "stop"],
                check=True,
                capture_output=True,
                timeout=15,
            )


@pytest.fixture
def source_database(market_postgres, monkeypatch):
    monkeypatch.setenv("PG_ENABLED", "1")
    monkeypatch.setenv("PG_DSN", market_postgres)
    with psycopg.connect(market_postgres) as connection:
        connection.execute("drop table if exists public.stock_hist_cache")
        connection.execute(
            "create table public.stock_hist_cache (symbol text, adjust text, date date, open float8, high float8, low float8, close float8, volume float8, amount float8, pct_chg float8, turnover_rate float8, amplitude float8, updated_at timestamptz, primary key(symbol,adjust,date))"
        )
    return market_postgres


def test_copy_excludes_real_upsert_and_committed_write_is_readable(source_database):
    row = {
        "symbol": "US:AAPL",
        "adjust": "qfq",
        "date": date(2026, 11, 20),
        "close": 11,
    }
    with psycopg.connect(source_database) as copy:
        copy.execute("select pg_advisory_xact_lock(%s)", (MARKET_DATA_KEY,))
        with pytest.raises(MarketDataBusy):
            upsert_rows("stock_hist_cache", [row], conflict_columns=("symbol", "adjust", "date"))
        cn = {**row, "symbol": "CN:000001"}
        assert upsert_rows("stock_hist_cache", [cn], conflict_columns=("symbol", "adjust", "date")) == 1
        assert copy.execute("select count(*) from stock_hist_cache where symbol like 'US:%'").fetchone()[0] == 0
    assert upsert_rows("stock_hist_cache", [row], conflict_columns=("symbol", "adjust", "date")) == 1
    with psycopg.connect(source_database) as reader:
        assert reader.execute("select close from stock_hist_cache where symbol = 'US:AAPL'").fetchone() == (11,)


def test_parallel_writer_transactions_exclude_copy_and_release_on_rollback(
    source_database,
):
    with (
        connect_postgres(market_write=True),
        connect_postgres(market_write=True),
        psycopg.connect(source_database) as reader,
    ):
        assert not reader.execute("select pg_try_advisory_xact_lock(%s)", (MARKET_DATA_KEY,)).fetchone()[0]
    with (
        pytest.raises(RuntimeError, match="rollback"),
        connect_postgres(market_write=True),
    ):
        raise RuntimeError("rollback")
    with psycopg.connect(source_database) as reader:
        assert reader.execute("select pg_try_advisory_xact_lock(%s)", (MARKET_DATA_KEY,)).fetchone()[0]
    with (
        pytest.raises(ValueError, match="显式事务"),
        connect_postgres(market_write=True, autocommit=True),
    ):
        pytest.fail("autocommit must not shed the protection before the write")


def test_whole_job_excludes_other_job_but_allows_independent_writes(source_database):
    with us_market_job():
        with pytest.raises(MarketDataBusy), us_market_job():
            pytest.fail("a second job was admitted")
        with connect_postgres(market_write=True) as writer:
            writer.execute(
                "insert into stock_hist_cache(symbol,adjust,date,close) values('US:AAPL','qfq','2026-11-20',11)"
            )
        with psycopg.connect(source_database) as reader:
            assert reader.execute("select count(*) from stock_hist_cache").fetchone()[0] == 1
            assert not reader.execute("select pg_try_advisory_xact_lock(%s)", (US_MARKET_JOB_KEY,)).fetchone()[0]
    with pytest.raises(RuntimeError), us_market_job():
        raise RuntimeError("job failed")
    with us_market_job():
        pass


def test_dead_owner_releases_job_lock(source_database):
    code = "from integrations.market_data_lock import us_market_job; import sys\nwith us_market_job():\n print('locked',flush=True);sys.stdin.readline()\n"
    process = subprocess.Popen(
        [sys.executable, "-c", code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline() == "locked\n"
        with pytest.raises(MarketDataBusy), us_market_job():
            pytest.fail("live owner was ignored")
        process.kill()
        process.wait(timeout=5)
        with us_market_job():
            pass
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()


def test_us_cli_requires_pg_while_cn_help_and_dry_run_remain_offline(monkeypatch):
    monkeypatch.setenv("PG_ENABLED", "0")
    for arguments in (
        ["--market", "cn"],
        ["--market", "hk"],
        ["--help"],
        ["--dry-run"],
    ):
        with cli_market_job(arguments):
            pass
    with pytest.raises(ValueError, match="不会回退"), cli_market_job(["refresh"]):
        pytest.fail("US write entrypoint fell back to an uncoordinated backend")


@pytest.fixture
def adjusted_provider(monkeypatch):
    import yfinance

    from core import stock_cache
    from integrations import data_source

    class FrozenClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 21, tzinfo=UTC).astimezone(tz)

    monkeypatch.setattr(stock_cache, "datetime", FrozenClock)

    days = pd.bdate_range(end="2026-11-20", periods=800, name="Date")
    frame = pd.DataFrame(
        {"Open": 10.0, "High": 12.0, "Low": 9.0, "Close": 11.0, "Volume": 1000.0},
        index=days,
    )

    def download(symbols, **options):
        assert options["auto_adjust"] is True
        values = symbols.split()
        if len(values) > 1:
            return pd.concat({symbol: frame for symbol in values}, axis=1)
        return frame.copy()

    monkeypatch.setattr(yfinance, "download", download)
    monkeypatch.setattr(data_source, "yahoo_first_trade_date", lambda symbol: None)
    return frame


def test_real_research_update_writes_pool_and_three_distinct_benchmarks(source_database, adjusted_provider):
    from scripts.us_research_refresh import refresh_research

    result = refresh_research(["AAPL", "MSFT"], date(2026, 11, 20), sleep_seconds=0)
    assert result["status"] == "completed" and result["missing_target"] == [] and result["quality_checked"] is False
    assert set(result["benchmarks"]) == {"SPY", "QQQ", "IWM"}
    with psycopg.connect(source_database) as reader:
        rows = reader.execute("select symbol,count(*) from stock_hist_cache group by symbol").fetchall()
    assert {symbol for symbol, _ in rows} == {
        "US:AAPL",
        "US:MSFT",
        "INDEX:US:SPY",
        "INDEX:US:QQQ",
        "INDEX:US:IWM",
    }
    assert all(count >= 320 for _, count in rows)


def test_stale_preflight_prevents_all_writes(source_database, adjusted_provider, monkeypatch):
    import yfinance

    from scripts.us_research_refresh import refresh_research

    monkeypatch.setattr(yfinance, "download", lambda *args, **kwargs: adjusted_provider.iloc[:-1].copy())
    with pytest.raises(RuntimeError):
        refresh_research(["AAPL"], date(2026, 11, 20), sleep_seconds=0)
    with psycopg.connect(source_database) as reader:
        assert reader.execute("select count(*) from stock_hist_cache").fetchone()[0] == 0


def test_missing_symbol_retains_confirmed_writes_but_returns_incomplete(
    source_database, adjusted_provider, monkeypatch
):
    import yfinance

    from scripts.us_research_refresh import refresh_research

    monkeypatch.setattr(yfinance, "download", lambda *args, **kwargs: adjusted_provider.copy())
    result = refresh_research(["AAPL", "MSFT"], date(2026, 11, 20), sleep_seconds=0)
    assert result["status"] == "incomplete" and result["missing_target"] == [
        "AAPL",
        "MSFT",
    ]
    with psycopg.connect(source_database) as reader:
        assert reader.execute("select count(distinct symbol) from stock_hist_cache").fetchone()[0] == 3


@pytest.mark.parametrize("raw", ["", "AAPL,,MSFT", "$AAPL", "../MSFT", "AAPL;rm"])
def test_invalid_explicit_pool_never_falls_back_to_sp500(raw):
    from scripts.us_research_refresh import research_symbols

    with pytest.raises(ValueError):
        research_symbols(raw)


def test_research_pool_normalizes_symbols_without_losing_identity():
    from scripts.us_research_refresh import research_symbols

    assert research_symbols("aapl,US:BRK.B,AAPL") == ["AAPL", "BRK-B"]
