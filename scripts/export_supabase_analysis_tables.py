from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from integrations.supabase_base import create_admin_client
from scripts.postgres_analysis_tables import TABLE_SPEC_MAP, TABLE_SPECS


def _json_default(value):
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _is_missing_column_error(err: Exception) -> bool:
    text = str(err).lower()
    return "does not exist" in text or "could not find the" in text or "42703" in text


def _is_missing_table_error(err: Exception) -> bool:
    text = str(err).lower()
    return "pgrst205" in text or "could not find the table" in text


def _iter_order_column_sets(order_columns: Iterable[str]) -> list[tuple[str, ...]]:
    cols = list(order_columns)
    variants: list[tuple[str, ...]] = []
    while True:
        variants.append(tuple(cols))
        if not cols:
            break
        cols = cols[:-1]
    return variants


def _export_table(table: str, output_dir: Path, batch_size: int) -> int:
    spec = TABLE_SPEC_MAP[table]
    client = create_admin_client()
    output_path = output_dir / f"{table}.jsonl"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    start = 0

    with output_path.open("w", encoding="utf-8") as fh:
        while True:
            resp = None
            last_error: Exception | None = None
            for order_cols in _iter_order_column_sets(spec.order_columns):
                try:
                    query = client.table(table).select("*")
                    for col in order_cols:
                        query = query.order(col)
                    resp = query.range(start, start + batch_size - 1).execute()
                    break
                except Exception as err:
                    last_error = err
                    if _is_missing_table_error(err):
                        print(f"[export] skip table={table} reason=missing_table", flush=True)
                        return 0
                    if order_cols and _is_missing_column_error(err):
                        print(
                            f"[export] table={table} fallback_without_order_col={order_cols[-1]}",
                            flush=True,
                        )
                        continue
                    raise
            if resp is None:
                if last_error is not None:
                    raise last_error
                raise RuntimeError(f"export failed without response for table={table}")
            rows = resp.data or []
            if not rows:
                break
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False, default=_json_default))
                fh.write("\n")
            total += len(rows)
            start += batch_size
            print(f"[export] table={table} rows={total}", flush=True)

    return total


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export analysis-related Supabase tables to JSONL files."
    )
    parser.add_argument(
        "--output-dir",
        default="data/pg_migration_export",
        help="Directory to write <table>.jsonl files into.",
    )
    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Export only the specified table(s). Can be passed multiple times.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Supabase range pagination batch size.",
    )
    args = parser.parse_args()

    selected = args.tables or [spec.name for spec in TABLE_SPECS]
    unknown = [name for name in selected if name not in TABLE_SPEC_MAP]
    if unknown:
        raise SystemExit(f"Unknown table(s): {', '.join(unknown)}")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, int] = {}
    for table in selected:
        count = _export_table(table, output_dir, max(int(args.batch_size), 1))
        manifest[table] = count
    manifest_path = output_dir / "_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[export] done manifest={manifest_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
