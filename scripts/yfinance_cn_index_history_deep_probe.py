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


TARGETS: list[tuple[str, str]] = [
    ("上证指数", "000001.SS"),
    ("深证成指", "399001.SZ"),
    ("创业板指", "399006.SZ"),
    ("沪深300", "000300.SS"),
    ("中证500", "000905.SS"),
    ("科创50", "000688.SS"),
]

DOWNLOAD_PERIODS = ["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"]
HISTORY_PERIODS = ["5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "max"]


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


def _frame_summary(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None or df.empty:
        return {"rows": 0, "columns": [], "first_date": None, "last_date": None}
    work = _flatten_columns(df).reset_index()
    work = _flatten_columns(work)
    date_col = "Date" if "Date" in work.columns else ("index" if "index" in work.columns else None)
    return {
        "rows": int(len(work)),
        "columns": [str(c) for c in work.columns.tolist()],
        "first_date": _jsonable(work.iloc[0].get(date_col) if date_col else None),
        "last_date": _jsonable(work.iloc[-1].get(date_col) if date_col else None),
    }


def _safe_call(label: str, fn) -> dict[str, Any]:
    try:
        df = fn()
        return {"ok": True, "label": label, **_frame_summary(df)}
    except Exception as exc:
        return {"ok": False, "label": label, "error": f"{type(exc).__name__}: {exc}"}


def _probe_symbol(symbol: str, start_s: str, end_s: str) -> dict[str, Any]:
    import yfinance as yf

    ticker = yf.Ticker(symbol)
    end_open = (
        pd.to_datetime(end_s, errors="coerce") + pd.Timedelta(days=1)
    ).strftime("%Y-%m-%d")

    result: dict[str, Any] = {
        "download_window": _safe_call(
            f"download(start={start_s}, end={end_s})",
            lambda: yf.download(
                symbol,
                start=start_s,
                end=end_open,
                interval="1d",
                auto_adjust=True,
                progress=False,
            ),
        ),
        "download_periods": {},
        "history_periods": {},
    }

    for period in DOWNLOAD_PERIODS:
        result["download_periods"][period] = _safe_call(
            f"download(period={period})",
            lambda period=period: yf.download(
                symbol,
                period=period,
                interval="1d",
                auto_adjust=True,
                progress=False,
            ),
        )
    for period in HISTORY_PERIODS:
        result["history_periods"][period] = _safe_call(
            f"Ticker.history(period={period})",
            lambda period=period: ticker.history(
                period=period,
                interval="1d",
                auto_adjust=True,
            ),
        )
    return result


def run_probe(start_s: str, end_s: str) -> dict[str, Any]:
    import yfinance as yf

    summary: dict[str, Any] = {
        "yfinance_version": getattr(yf, "__version__", "unknown"),
        "start": start_s,
        "end": end_s,
        "symbols": {},
    }
    for name, symbol in TARGETS:
        summary["symbols"][symbol] = {
            "name": name,
            **_probe_symbol(symbol, start_s, end_s),
        }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deep probe for A-share index history coverage in yfinance"
    )
    parser.add_argument(
        "--start",
        default=(date.today() - timedelta(days=45)).isoformat(),
    )
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
