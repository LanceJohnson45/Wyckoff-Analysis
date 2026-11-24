"""PostgreSQL protocol shared by US maintenance and research copies."""

from __future__ import annotations

import argparse
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

# Database-scoped keys stay independent of host aliases, branch, date and credentials.
US_MARKET_JOB_KEY = int.from_bytes(b"WYKUS01", "big")
MARKET_DATA_KEY = int.from_bytes(b"WYKDAT1", "big")
US_CACHE_PREFIXES = ("US:", "INDEX:US:")


class MarketDataBusy(RuntimeError):
    """A cooperating maintenance/copy owns this database's market data."""


def protect_market_write(cursor) -> None:
    """Use a separate shared transaction lock so parallel job writers do not self-lock."""
    cursor.execute("select pg_try_advisory_xact_lock_shared(%s) as acquired", (MARKET_DATA_KEY,))
    if not cursor.fetchone()["acquired"]:
        raise MarketDataBusy("行情复制正在运行；本次缓存写入未执行，请稍后按原目标日重试")


@contextmanager
def us_market_job() -> Iterator[None]:
    """Hold one transaction across the job's independent writer transactions."""
    from integrations.postgres_base import connect_postgres, postgres_enabled

    if not postgres_enabled():
        raise ValueError("US维护需PG_ENABLED=1及PG_*；不会回退到未参与协调的Supabase写入")
    with connect_postgres() as connection, connection.cursor() as cursor:
        cursor.execute("select pg_try_advisory_xact_lock(%s) as acquired", (US_MARKET_JOB_KEY,))
        if not cursor.fetchone()["acquired"]:
            raise MarketDataBusy("同一数据库的US行情维护或研究复制正在运行；请按原目标日重试")
        yield


@contextmanager
def cli_market_job(arguments: Sequence[str], *, default_market: str = "us") -> Iterator[None]:
    """Only the executable US entrypoint takes the coarse job lock; CN/HK stay independent."""
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--market", default=default_market)
    parser.add_argument("--dry-run", action="store_true")
    args, _ = parser.parse_known_args(arguments)
    if (
        str(args.market).strip().lower() != "us"
        or args.dry_run
        or any(value in arguments for value in ("-h", "--help"))
    ):
        yield
    else:
        with us_market_job():
            yield
