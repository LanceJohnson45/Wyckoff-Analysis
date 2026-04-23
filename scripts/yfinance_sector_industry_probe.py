from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from typing import Any

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


DEFAULT_TARGETS: dict[str, list[str]] = {
    "us": ["MSFT", "AAPL", "NVDA"],
    "hk": ["0700.HK", "9988.HK", "1810.HK"],
    "cn": ["600519.SS", "000001.SZ", "300750.SZ"],
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
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


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


def _pick_info_fields(info: dict[str, Any]) -> dict[str, Any]:
    wanted = [
        "symbol",
        "shortName",
        "longName",
        "quoteType",
        "exchange",
        "market",
        "sector",
        "sectorKey",
        "industry",
        "industryKey",
        "industryDisp",
        "country",
        "currency",
        "marketCap",
    ]
    return {key: _jsonable(info.get(key)) for key in wanted if key in info}


def _currency_to_cny_rate(currency: str) -> float:
    cur = str(currency or "").strip().upper()
    if cur in {"CNY", "CNH", "RMB"}:
        return 1.0
    if cur == "HKD":
        try:
            return float(os.getenv("HKD_CNY_RATE", "0.92"))
        except Exception:
            return 0.92
    if cur == "USD":
        try:
            return float(os.getenv("USD_CNY_RATE", "7.20"))
        except Exception:
            return 7.20
    try:
        return float(os.getenv(f"{cur}_CNY_RATE", "1.0"))
    except Exception:
        return 1.0


def _probe_sector_or_industry(obj: Any, *, kind: str) -> dict[str, Any]:
    if obj is None:
        return {"ok": False, "error": f"{kind}_object_missing"}
    out: dict[str, Any] = {
        "ok": True,
        "class": type(obj).__name__,
    }
    ticker = getattr(obj, "ticker", None)
    out["ticker"] = _jsonable(ticker)
    try:
        info = getattr(ticker, "info", {}) if ticker is not None else {}
        out["ticker_info"] = _pick_info_fields(info if isinstance(info, dict) else {})
    except Exception as exc:
        out["ticker_info_error"] = f"{type(exc).__name__}: {exc}"
    try:
        hist = ticker.history(period="1mo") if ticker is not None else pd.DataFrame()
        out["ticker_history"] = _preview_df(hist.reset_index() if not hist.empty else hist)
    except Exception as exc:
        out["ticker_history_error"] = f"{type(exc).__name__}: {exc}"
    return out


def _probe_symbol(symbol: str) -> dict[str, Any]:
    import yfinance as yf

    out: dict[str, Any] = {"symbol": symbol}
    ticker = yf.Ticker(symbol)
    try:
        info = ticker.info
        if not isinstance(info, dict):
            info = {}
        out["ticker_ok"] = True
        out["ticker_info"] = _pick_info_fields(info)
    except Exception as exc:
        out["ticker_ok"] = False
        out["ticker_error"] = f"{type(exc).__name__}: {exc}"
        out["ticker_info"] = {}
        info = {}

    sector_key = str(info.get("sectorKey") or "").strip()
    industry_key = str(info.get("industryKey") or "").strip()
    currency = str(info.get("currency") or "").strip().upper()
    market_cap = info.get("marketCap")
    try:
        market_cap_float = float(market_cap)
    except Exception:
        market_cap_float = None
    market_cap_yi = None
    if market_cap_float is not None and market_cap_float > 0:
        market_cap_yi = market_cap_float * _currency_to_cny_rate(currency) / 1e8
    out["sector_key"] = sector_key or None
    out["industry_key"] = industry_key or None
    out["market_cap"] = market_cap_float
    out["market_cap_currency"] = currency or None
    out["market_cap_yi"] = market_cap_yi

    try:
        hist = ticker.history(period="1mo", auto_adjust=True)
        out["ticker_history"] = _preview_df(hist.reset_index() if not hist.empty else hist)
    except Exception as exc:
        out["ticker_history_error"] = f"{type(exc).__name__}: {exc}"

    if sector_key:
        try:
            out["sector_probe"] = _probe_sector_or_industry(
                yf.Sector(sector_key), kind="sector"
            )
        except Exception as exc:
            out["sector_probe"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    else:
        out["sector_probe"] = {"ok": False, "error": "sectorKey_missing"}

    if industry_key:
        try:
            out["industry_probe"] = _probe_sector_or_industry(
                yf.Industry(industry_key), kind="industry"
            )
        except Exception as exc:
            out["industry_probe"] = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
    else:
        out["industry_probe"] = {"ok": False, "error": "industryKey_missing"}
    return out


def _build_targets_from_args(args: argparse.Namespace) -> dict[str, list[str]]:
    if args.symbol:
        grouped: dict[str, list[str]] = {}
        for item in args.symbol:
            market, symbol = item.split(":", 1) if ":" in item else ("custom", item)
            grouped.setdefault(market.strip().lower(), []).append(symbol.strip())
        return grouped
    return {
        market: list(symbols)
        for market, symbols in DEFAULT_TARGETS.items()
        if market in set(args.markets)
    }


def run_probe(targets: dict[str, list[str]]) -> dict[str, Any]:
    import yfinance as yf

    summary: dict[str, Any] = {
        "yfinance_version": getattr(yf, "__version__", "unknown"),
        "targets": targets,
        "tests": {},
    }
    for market, symbols in targets.items():
        summary["tests"][market] = {}
        for symbol in symbols:
            try:
                summary["tests"][market][symbol] = _probe_symbol(symbol)
            except Exception as exc:
                summary["tests"][market][symbol] = {
                    "symbol": symbol,
                    "ticker_ok": False,
                    "probe_error": f"{type(exc).__name__}: {exc}",
                }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe yfinance sector/industry support for CN/HK/US tickers"
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["cn", "hk", "us"],
        choices=["cn", "hk", "us"],
        help="Markets to test when --symbol is not provided",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Custom symbol in form market:SYMBOL, e.g. us:MSFT or hk:0700.HK",
    )
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    targets = _build_targets_from_args(args)
    payload = json.dumps(_jsonable(run_probe(targets)), ensure_ascii=False, indent=2)
    print(payload)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
