from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


INDEX_CANDIDATES: dict[str, list[str]] = {
    "sse_composite": ["000001.SS", "000001.SH", "000001.SZ"],
    "szse_component": ["399001.SZ", "399001.SS"],
    "chinext": ["399006.SZ", "399006.SS"],
    "csi300": ["000300.SS", "399300.SZ"],
    "csi500": ["000905.SS", "399905.SZ"],
    "star50": ["000688.SS"],
}


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.columns = [
        "__".join(str(part) for part in col if str(part) != "")
        if isinstance(col, tuple)
        else str(col)
        for col in work.columns
    ]
    return work


def _probe_symbol(symbol: str, start_s: str, end_s: str) -> dict[str, Any]:
    try:
        import yfinance as yf

        end_open = (
            pd.to_datetime(end_s, errors="coerce") + pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        df = yf.download(
            symbol,
            start=start_s,
            end=end_open,
            interval="1d",
            auto_adjust=True,
            progress=False,
        )
        if df is None or df.empty:
            return {"ok": False, "error": "empty"}
        work = _flatten_columns(df).reset_index()
        work = _flatten_columns(work)
        date_col = "Date" if "Date" in work.columns else ("index" if "index" in work.columns else None)
        return {
            "ok": True,
            "rows": int(len(work)),
            "columns": [str(c) for c in work.columns.tolist()],
            "first_date": _jsonable(work.iloc[0].get(date_col) if date_col else None),
            "last_date": _jsonable(work.iloc[-1].get(date_col) if date_col else None),
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def run_matrix(start_s: str, end_s: str) -> dict[str, Any]:
    import yfinance as yf

    summary: dict[str, Any] = {
        "yfinance_version": getattr(yf, "__version__", "unknown"),
        "start": start_s,
        "end": end_s,
        "groups": {},
    }
    for group, symbols in INDEX_CANDIDATES.items():
        summary["groups"][group] = {
            symbol: _probe_symbol(symbol, start_s, end_s) for symbol in symbols
        }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe candidate yfinance tickers for A-share indexes"
    )
    parser.add_argument("--start", default=(date.today() - timedelta(days=45)).isoformat())
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    summary = run_matrix(args.start, args.end)
    payload = json.dumps(_jsonable(summary), ensure_ascii=False, indent=2)
    print(payload)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
