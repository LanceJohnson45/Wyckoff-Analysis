#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.data_source import fetch_stock_hist
from integrations.postgres_base import connect_postgres, postgres_enabled


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _parse_symbols(raw: str) -> list[str]:
    return [s.strip().upper() for s in str(raw or "").split(",") if s.strip()]


def _probe_postgres() -> None:
    with connect_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute("select current_database(), current_user, now()")
            row = cur.fetchone()
    if isinstance(row, dict):
        db_name = row.get("current_database")
        db_user = row.get("current_user")
        db_now = row.get("now")
    else:
        db_name = row[0] if row else None
        db_user = row[1] if row and len(row) > 1 else None
        db_now = row[2] if row and len(row) > 2 else None
    _log(f"postgres ok db={db_name} user={db_user} now={db_now}")


def main() -> int:
    parser = argparse.ArgumentParser(description="US data source smoke test")
    parser.add_argument(
        "--symbols",
        default="AAPL,MSFT,GM,EOG",
        help="Comma-separated US tickers",
    )
    parser.add_argument(
        "--start-date",
        default="2025-09-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default="2025-09-10",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="Skip PostgreSQL connectivity probe",
    )
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols)
    if not symbols:
        raise SystemExit("symbols is empty")

    os.environ.setdefault("FUNNEL_MARKET", "us")
    os.environ.setdefault("DATA_SOURCE_DEBUG", "1")

    _log(
        "env "
        f"pg_enabled={postgres_enabled()} "
        f"pg_host_set={bool(os.getenv('PG_HOST', '').strip())} "
        f"tickflow_key_set={bool(os.getenv('TICKFLOW_API_KEY', '').strip())}"
    )

    if args.skip_db:
        _log("postgres skipped by --skip-db")
    elif postgres_enabled():
        try:
            _probe_postgres()
        except Exception as e:
            _log(f"postgres fail {type(e).__name__}: {e}")
    else:
        _log("postgres skipped because PG_ENABLED is false")

    ok = 0
    fail = 0
    for symbol in symbols:
        try:
            df = fetch_stock_hist(
                symbol,
                args.start_date,
                args.end_date,
                adjust="qfq",
                market="us",
            )
            source = df.attrs.get("source")
            first_date = None if df.empty else str(df.iloc[0]["日期"])
            last_date = None if df.empty else str(df.iloc[-1]["日期"])
            _log(
                f"fetch ok {symbol} rows={len(df.index)} "
                f"source={source} first={first_date} last={last_date}"
            )
            ok += 1
        except Exception as e:
            _log(f"fetch fail {symbol} {type(e).__name__}: {e}")
            fail += 1

    _log(f"smoke done ok={ok} fail={fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
