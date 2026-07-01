from __future__ import annotations

import argparse
import json
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from scripts.postgres_analysis_tables import TABLE_SPEC_MAP, TABLE_SPECS, TableSpec


def _adapt_row(row: dict, spec: TableSpec) -> dict:
    out: dict = {}
    json_cols = set(spec.json_columns)
    for key, value in row.items():
        if key in json_cols and value is not None:
            out[key] = Jsonb(value)
        else:
            out[key] = value
    return out

def _upsert_sql(table: str, columns: list[str], conflict_columns: tuple[str, ...]) -> str:
    quoted_cols = ", ".join(f'"{col}"' for col in columns)
    placeholders = ", ".join(f"%({col})s" for col in columns)
    conflict = ", ".join(f'"{col}"' for col in conflict_columns)
    update_cols = [col for col in columns if col not in conflict_columns]
    if update_cols:
        updates = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in update_cols)
        action = f"do update set {updates}"
    else:
        action = "do nothing"
    return (
        f'insert into public."{table}" ({quoted_cols}) '
        f"values ({placeholders}) "
        f"on conflict ({conflict}) {action}"
    )


def _import_table(
    conn: psycopg.Connection,
    table: str,
    input_dir: Path,
    batch_size: int,
) -> int:
    spec = TABLE_SPEC_MAP[table]
    input_path = input_dir / f"{table}.jsonl"
    if not input_path.exists():
        print(f"[import] skip table={table} reason=file_missing path={input_path}", flush=True)
        return 0

    rows: list[dict] = []
    total = 0
    sql: str | None = None

    with input_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            raw = json.loads(text)
            if not isinstance(raw, dict):
                continue
            row = _adapt_row(raw, spec)
            if sql is None:
                sql = _upsert_sql(table, list(row.keys()), spec.conflict_columns)
            rows.append(row)
            if len(rows) >= batch_size:
                with conn.cursor() as cur:
                    cur.executemany(sql, rows)
                total += len(rows)
                conn.commit()
                print(f"[import] table={table} rows={total}", flush=True)
                rows.clear()

    if rows:
        if sql is None:
            sql = _upsert_sql(table, list(rows[0].keys()), spec.conflict_columns)
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        total += len(rows)
        conn.commit()
        print(f"[import] table={table} rows={total}", flush=True)

    if spec.reset_sequence_column:
        seq_sql = (
            "select setval("
            "pg_get_serial_sequence(%s, %s), "
            "coalesce((select max("
            f'"{spec.reset_sequence_column}"'
            f') from public."{table}"), 1), true)'
        )
        with conn.cursor() as cur:
            cur.execute(seq_sql, (f"public.{table}", spec.reset_sequence_column))
        conn.commit()

    return total


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import exported JSONL analysis tables into PostgreSQL."
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL DSN, e.g. postgresql://user:pass@host:5432/dbname",
    )
    parser.add_argument(
        "--input-dir",
        default="data/pg_migration_export",
        help="Directory containing <table>.jsonl export files.",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Import only the specified table(s). Can be passed multiple times.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per executemany batch.",
    )
    args = parser.parse_args()

    selected = args.tables or [spec.name for spec in TABLE_SPECS]
    unknown = [name for name in selected if name not in TABLE_SPEC_MAP]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}")

    input_dir = Path(args.input_dir).resolve()
    with psycopg.connect(args.dsn) as conn:
        for table in selected:
            _import_table(conn, table, input_dir, max(int(args.batch_size), 1))
    print("[import] done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
