from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


DEFAULT_SYMBOLS: dict[str, str] = {
    "a": "600519.SS",
    "us": "AAPL",
    "hk": "0700.HK",
}


@dataclass(frozen=True)
class InterfaceSpec:
    name: str
    access: str
    kwargs: dict[str, Any] | None = None


def _default_shares_window() -> dict[str, str]:
    end = date.today()
    start = end - timedelta(days=400)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
    }


INTERFACES: list[InterfaceSpec] = [
    InterfaceSpec("get_isin", "method"),
    InterfaceSpec("isin", "property"),
    InterfaceSpec(
        "history",
        "method",
        {"period": "6mo", "interval": "1d", "auto_adjust": False},
    ),
    InterfaceSpec("get_history_metadata", "method"),
    InterfaceSpec("get_dividends", "method"),
    InterfaceSpec("dividends", "property"),
    InterfaceSpec("get_splits", "method"),
    InterfaceSpec("splits", "property"),
    InterfaceSpec("get_actions", "method"),
    InterfaceSpec("actions", "property"),
    InterfaceSpec("get_capital_gains", "method"),
    InterfaceSpec("capital_gains", "property"),
    InterfaceSpec("get_shares_full", "method", _default_shares_window()),
    InterfaceSpec("get_info", "method"),
    InterfaceSpec("info", "property"),
    InterfaceSpec("get_fast_info", "method"),
    InterfaceSpec("fast_info", "property"),
    InterfaceSpec("get_news", "method"),
    InterfaceSpec("news", "property"),
    InterfaceSpec("get_income_stmt", "method"),
    InterfaceSpec("income_stmt", "property"),
    InterfaceSpec("quarterly_income_stmt", "property"),
    InterfaceSpec("ttm_income_stmt", "property"),
    InterfaceSpec("get_balance_sheet", "method"),
    InterfaceSpec("balance_sheet", "property"),
    InterfaceSpec("get_cashflow", "method"),
    InterfaceSpec("cashflow", "property"),
    InterfaceSpec("quarterly_cashflow", "property"),
    InterfaceSpec("ttm_cashflow", "property"),
    InterfaceSpec("get_earnings", "method"),
    InterfaceSpec("earnings", "property"),
    InterfaceSpec("calendar", "property"),
    InterfaceSpec("get_earnings_dates", "method", {"limit": 12}),
    InterfaceSpec("earnings_dates", "property"),
    InterfaceSpec("get_sec_filings", "method"),
    InterfaceSpec("sec_filings", "property"),
    InterfaceSpec("get_recommendations", "method"),
    InterfaceSpec("recommendations", "property"),
    InterfaceSpec("get_recommendations_summary", "method"),
    InterfaceSpec("recommendations_summary", "property"),
    InterfaceSpec("get_upgrades_downgrades", "method"),
    InterfaceSpec("upgrades_downgrades", "property"),
    InterfaceSpec("get_sustainability", "method"),
    InterfaceSpec("sustainability", "property"),
    InterfaceSpec("get_analyst_price_targets", "method"),
    InterfaceSpec("analyst_price_targets", "property"),
    InterfaceSpec("get_earnings_estimate", "method"),
    InterfaceSpec("earnings_estimate", "property"),
    InterfaceSpec("get_revenue_estimate", "method"),
    InterfaceSpec("revenue_estimate", "property"),
    InterfaceSpec("get_earnings_history", "method"),
    InterfaceSpec("earnings_history", "property"),
    InterfaceSpec("get_eps_trend", "method"),
    InterfaceSpec("eps_trend", "property"),
    InterfaceSpec("get_eps_revisions", "method"),
    InterfaceSpec("eps_revisions", "property"),
    InterfaceSpec("get_growth_estimates", "method"),
    InterfaceSpec("growth_estimates", "property"),
    InterfaceSpec("get_funds_data", "method"),
    InterfaceSpec("funds_data", "property"),
    InterfaceSpec("get_insider_purchases", "method"),
    InterfaceSpec("insider_purchases", "property"),
    InterfaceSpec("get_insider_transactions", "method"),
    InterfaceSpec("insider_transactions", "property"),
    InterfaceSpec("get_insider_roster_holders", "method"),
    InterfaceSpec("insider_roster_holders", "property"),
    InterfaceSpec("get_major_holders", "method"),
    InterfaceSpec("major_holders", "property"),
    InterfaceSpec("get_institutional_holders", "method"),
    InterfaceSpec("institutional_holders", "property"),
    InterfaceSpec("get_mutualfund_holders", "method"),
    InterfaceSpec("mutualfund_holders", "property"),
]


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime, pd.Timestamp, pd.Timedelta)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "tolist"):
        try:
            return value.tolist()
        except Exception:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _truncate_text(text: str, *, max_len: int = 600) -> str:
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}...(truncated)"


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work.columns = [
        "__".join(str(part) for part in col if str(part) != "")
        if isinstance(col, tuple)
        else str(col)
        for col in work.columns
    ]
    return work


def _series_summary(series: pd.Series, *, sample_size: int) -> dict[str, Any]:
    sample = series.head(sample_size)
    return {
        "kind": "series",
        "length": int(len(series)),
        "name": str(series.name) if series.name is not None else None,
        "dtype": str(series.dtype),
        "index_type": type(series.index).__name__,
        "sample": [_jsonable(v) for v in sample.tolist()],
    }


def _frame_summary(df: pd.DataFrame, *, sample_size: int) -> dict[str, Any]:
    work = _normalize_frame(df).reset_index()
    work = _normalize_frame(work)
    sample = work.head(sample_size).copy()
    for col in sample.columns:
        sample[col] = sample[col].map(_jsonable)
    return {
        "kind": "dataframe",
        "rows": int(len(df)),
        "columns": [str(c) for c in work.columns.tolist()],
        "dtypes": {str(col): str(dtype) for col, dtype in work.dtypes.items()},
        "sample": sample.to_dict("records"),
    }


def _mapping_summary(data: dict[Any, Any], *, sample_size: int) -> dict[str, Any]:
    keys = list(data.keys())
    sample_keys = keys[:sample_size]
    return {
        "kind": "dict",
        "size": int(len(data)),
        "sample_keys": [str(k) for k in sample_keys],
        "sample_items": {
            str(k): _jsonable(data[k])
            for k in sample_keys
        },
    }


def _sequence_summary(data: list[Any] | tuple[Any, ...], *, sample_size: int) -> dict[str, Any]:
    return {
        "kind": "sequence",
        "length": int(len(data)),
        "sample": [_jsonable(v) for v in list(data[:sample_size])],
    }


def _object_summary(value: Any, *, sample_size: int) -> dict[str, Any]:
    public_attrs = [
        name for name in dir(value)
        if not name.startswith("_")
    ]
    sample_attrs = public_attrs[:sample_size]
    attrs: dict[str, Any] = {}
    for name in sample_attrs:
        try:
            attr_value = getattr(value, name)
            if callable(attr_value):
                continue
            attrs[name] = _jsonable(attr_value)
        except Exception as exc:
            attrs[name] = f"{type(exc).__name__}: {exc}"
    return {
        "kind": "object",
        "class": type(value).__name__,
        "module": type(value).__module__,
        "repr": _truncate_text(repr(value)),
        "public_attr_count": int(len(public_attrs)),
        "sample_attrs": attrs,
    }


def summarize_value(value: Any, *, sample_size: int) -> dict[str, Any]:
    if isinstance(value, pd.DataFrame):
        return _frame_summary(value, sample_size=sample_size)
    if isinstance(value, pd.Series):
        return _series_summary(value, sample_size=sample_size)
    if isinstance(value, dict):
        return _mapping_summary(value, sample_size=sample_size)
    if isinstance(value, (list, tuple)):
        return _sequence_summary(value, sample_size=sample_size)
    if value is None or isinstance(value, (str, int, float, bool, Decimal)):
        return {
            "kind": "scalar",
            "value": _jsonable(value),
        }
    return _object_summary(value, sample_size=sample_size)


def _captured_side_effects(
    *,
    stdout_text: str,
    stderr_text: str,
    warning_items: list[warnings.WarningMessage],
) -> dict[str, Any]:
    side_effects: dict[str, Any] = {}
    if stdout_text.strip():
        side_effects["stdout"] = _truncate_text(stdout_text.strip())
    if stderr_text.strip():
        side_effects["stderr"] = _truncate_text(stderr_text.strip(), max_len=1200)
    if warning_items:
        side_effects["warnings"] = [
            _truncate_text(f"{item.category.__name__}: {item.message}", max_len=300)
            for item in warning_items
        ]
    return side_effects


def run_single_interface(
    ticker: Any,
    spec: InterfaceSpec,
    *,
    sample_size: int,
) -> dict[str, Any]:
    started = time.perf_counter()
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
            with warnings.catch_warnings(record=True) as warning_items:
                warnings.simplefilter("always")
                target = getattr(ticker, spec.name)
                if spec.access == "method":
                    if not callable(target):
                        raise TypeError(f"{spec.name} is not callable")
                    value = target(**(spec.kwargs or {}))
                else:
                    value = target
        elapsed = round(time.perf_counter() - started, 4)
        result = {
            "ok": True,
            "access": spec.access,
            "duration_seconds": elapsed,
            "kwargs": spec.kwargs or {},
            "summary": summarize_value(value, sample_size=sample_size),
        }
        result.update(
            _captured_side_effects(
                stdout_text=stdout_buffer.getvalue(),
                stderr_text=stderr_buffer.getvalue(),
                warning_items=warning_items,
            )
        )
        return result
    except Exception as exc:
        elapsed = round(time.perf_counter() - started, 4)
        result = {
            "ok": False,
            "access": spec.access,
            "duration_seconds": elapsed,
            "kwargs": spec.kwargs or {},
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        captured_warnings = locals().get("warning_items", [])
        result.update(
            _captured_side_effects(
                stdout_text=stdout_buffer.getvalue(),
                stderr_text=stderr_buffer.getvalue(),
                warning_items=captured_warnings,
            )
        )
        return result


def run_probe(
    symbols: dict[str, str],
    *,
    sample_size: int,
) -> dict[str, Any]:
    import yfinance as yf

    results: dict[str, dict[str, Any]] = {}
    totals = {
        "interfaces": int(len(INTERFACES)),
        "markets": int(len(symbols)),
        "calls": int(len(INTERFACES) * len(symbols)),
        "ok": 0,
        "failed": 0,
    }

    tickers = {market: yf.Ticker(symbol) for market, symbol in symbols.items()}
    for spec in INTERFACES:
        results[spec.name] = {}
        for market, symbol in symbols.items():
            outcome = run_single_interface(
                tickers[market],
                spec,
                sample_size=sample_size,
            )
            outcome["symbol"] = symbol
            results[spec.name][market] = outcome
            if outcome["ok"]:
                totals["ok"] += 1
            else:
                totals["failed"] += 1

    return {
        "generated_at": datetime.now().isoformat(),
        "yfinance_version": getattr(yf, "__version__", "unknown"),
        "symbols": symbols,
        "totals": totals,
        "interfaces": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe yfinance.Ticker interfaces across A-share, US, and HK symbols"
    )
    parser.add_argument("--a-symbol", default=DEFAULT_SYMBOLS["a"])
    parser.add_argument("--us-symbol", default=DEFAULT_SYMBOLS["us"])
    parser.add_argument("--hk-symbol", default=DEFAULT_SYMBOLS["hk"])
    parser.add_argument("--sample-size", type=int, default=3)
    parser.add_argument(
        "--output",
        default=str(
            Path(__file__).resolve().parent.parent
            / "data"
            / "yfinance_ticker_interfaces_probe.json"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    symbols = {
        "a": str(args.a_symbol).strip().upper(),
        "us": str(args.us_symbol).strip().upper(),
        "hk": str(args.hk_symbol).strip().upper(),
    }
    summary = run_probe(symbols, sample_size=max(1, int(args.sample_size)))
    payload = json.dumps(_jsonable(summary), ensure_ascii=False, indent=2)
    print(payload)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
