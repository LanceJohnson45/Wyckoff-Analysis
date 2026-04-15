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


def _preview_df(df: pd.DataFrame | None, *, rows: int = 3) -> dict[str, Any]:
    if df is None:
        return {"rows": 0, "columns": [], "sample": []}
    sample = df.head(rows).copy()
    for col in sample.columns:
        sample[col] = sample[col].map(_jsonable)
    return {
        "rows": int(len(df)),
        "columns": [str(c) for c in df.columns.tolist()],
        "sample": sample.to_dict("records"),
    }


def _pick_focus_columns(df: pd.DataFrame, keywords: list[str]) -> list[str]:
    cols: list[str] = []
    for col in df.columns:
        text = str(col).lower()
        if any(k.lower() in text for k in keywords):
            cols.append(str(col))
    return cols


def _safe_probe(symbol: str, start_s: str, end_s: str) -> dict[str, Any]:
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
        return {
            "ok": True,
            "rows": int(len(work)),
            "focus_columns": _pick_focus_columns(
                work,
                ["date", "open", "high", "low", "close", "volume"],
            ),
            "preview": _preview_df(work),
        }
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def build_default_targets() -> list[tuple[str, str]]:
    return [
        ("上证指数", "000001.SS"),
        ("深证成指", "399001.SZ"),
        ("创业板指", "399006.SZ"),
        ("沪深300", "000300.SS"),
        ("中证500", "000905.SS"),
        ("科创50", "000688.SS"),
    ]


def run_probe(start_s: str, end_s: str) -> dict[str, Any]:
    import yfinance as yf

    summary: dict[str, Any] = {
        "yfinance_version": getattr(yf, "__version__", "unknown"),
        "start": start_s,
        "end": end_s,
        "tests": {},
    }
    for name, symbol in build_default_targets():
        summary["tests"][symbol] = {
            "name": name,
            **_safe_probe(symbol, start_s, end_s),
        }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Probe A-share indexes via yfinance")
    parser.add_argument("--start", default=(date.today() - timedelta(days=45)).isoformat())
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    summary = run_probe(args.start, args.end)
    payload = json.dumps(_jsonable(summary), ensure_ascii=False, indent=2)
    print(payload)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
