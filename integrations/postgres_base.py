from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Any, Iterable, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.sql import SQL, Identifier, Placeholder
    from psycopg.types.json import Jsonb
except Exception:  # pragma: no cover - lazy runtime dependency
    psycopg = None
    dict_row = None
    SQL = Identifier = Placeholder = Jsonb = None


_TRUE_VALUES = {"1", "true", "yes", "on"}


def postgres_enabled() -> bool:
    return str(os.getenv("PG_ENABLED", "") or "").strip().lower() in _TRUE_VALUES


def _require_psycopg() -> None:
    if psycopg is None or dict_row is None or SQL is None or Identifier is None or Placeholder is None:
        raise RuntimeError("psycopg is required for PostgreSQL mode. Install with: pip install 'psycopg[binary]>=3.2.0'")


def postgres_configured() -> bool:
    if str(os.getenv("PG_DSN", "") or "").strip():
        return True
    required = ("PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD")
    return all(str(os.getenv(name, "") or "").strip() for name in required)


def get_postgres_dsn() -> str:
    direct = str(os.getenv("PG_DSN", "") or "").strip()
    if direct:
        return direct

    host = str(os.getenv("PG_HOST", "") or "").strip()
    port = str(os.getenv("PG_PORT", "5432") or "5432").strip()
    dbname = str(os.getenv("PG_DATABASE", "") or "").strip()
    user = str(os.getenv("PG_USER", "") or "").strip()
    password = str(os.getenv("PG_PASSWORD", "") or "").strip()
    sslmode = str(os.getenv("PG_SSLMODE", "require") or "require").strip()

    if not all((host, port, dbname, user, password)):
        raise ValueError("PostgreSQL env not fully configured: PG_HOST/PORT/DATABASE/USER/PASSWORD")

    return " ".join(
        [
            f"host={host}",
            f"port={port}",
            f"dbname={dbname}",
            f"user={user}",
            f"password={password}",
            f"sslmode={sslmode or 'require'}",
        ]
    )


@contextmanager
def connect_postgres(*, autocommit: bool = False):
    _require_psycopg()
    conn = psycopg.connect(get_postgres_dsn(), row_factory=dict_row)
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def adapt_json_columns(
    row: dict[str, Any],
    *,
    json_columns: Sequence[str] = (),
) -> dict[str, Any]:
    _require_psycopg()
    adapted: dict[str, Any] = {}
    json_col_set = set(json_columns)
    for key, value in row.items():
        if key in json_col_set and value is not None:
            adapted[key] = Jsonb(value)
        else:
            adapted[key] = value
    return adapted


def fetch_all(
    sql_text: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.execute(sql_text, params)
        return list(cur.fetchall())


def fetch_one(
    sql_text: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.execute(sql_text, params)
        return cur.fetchone()


def execute(
    sql_text: str,
    params: Sequence[Any] | dict[str, Any] | None = None,
) -> None:
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.execute(sql_text, params)


def executemany(
    sql_text: str,
    rows: Iterable[Sequence[Any] | dict[str, Any]],
) -> None:
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.executemany(sql_text, list(rows))


def upsert_rows(
    table: str,
    rows: list[dict[str, Any]],
    *,
    conflict_columns: Sequence[str],
    json_columns: Sequence[str] = (),
) -> int:
    if not rows:
        return 0

    columns = list(rows[0].keys())
    assignments = [col for col in columns if col not in set(conflict_columns)]
    insert_sql = SQL(
        "insert into public.{table} ({columns}) values ({values}) on conflict ({conflict}) {action}"
    ).format(
        table=Identifier(table),
        columns=SQL(", ").join(Identifier(col) for col in columns),
        values=SQL(", ").join(Placeholder(col) for col in columns),
        conflict=SQL(", ").join(Identifier(col) for col in conflict_columns),
        action=(
            SQL("do update set ")
            + SQL(", ").join(
                SQL("{col} = EXCLUDED.{col}").format(col=Identifier(col))
                for col in assignments
            )
            if assignments
            else SQL("do nothing")
        ),
    )

    payload = [adapt_json_columns(row, json_columns=json_columns) for row in rows]
    with connect_postgres() as conn, conn.cursor() as cur:
        cur.executemany(insert_sql, payload)
    return len(payload)
