# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.wyckoff_engine import normalize_hist_from_fetch
from integrations.data_source import (
    _cn_index_to_yfinance_symbol,
    _fetch_index_akshare,
    _fetch_index_yfinance,
    fetch_index_hist,
    fetch_market_cap_map,
)
from integrations.yfinance_enrichment import build_market_cap_map_from_shares
from integrations.fetch_a_share_csv import _resolve_trading_window
from integrations.stock_hist_repository import get_stock_hist


def _date_arg(value: str | None, fallback: date) -> date:
    if not value:
        return fallback
    return pd.to_datetime(value, errors="raise").date()


def _frame_summary(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None or df.empty:
        return {"rows": 0}
    out = {"rows": int(len(df))}
    if "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce").dropna()
        if not dates.empty:
            out["first_date"] = dates.min().date().isoformat()
            out["last_date"] = dates.max().date().isoformat()
    for col in ("close", "volume"):
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce").dropna()
            if not values.empty:
                out[f"last_{col}"] = float(values.iloc[-1])
    return out


def _run_probe(name: str, fn: Callable[[], Any]) -> dict[str, Any]:
    try:
        value = fn()
        if isinstance(value, pd.DataFrame):
            return {"name": name, "ok": True, "summary": _frame_summary(value)}
        return {"name": name, "ok": True, "summary": value}
    except Exception as exc:
        return {
            "name": name,
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Probe CN index and market-cap data sources used by funnel."
    )
    parser.add_argument("--index-code", default="399006")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default="")
    parser.add_argument(
        "--symbols",
        default="000001,000002,300750,600519",
        help="Comma-separated CN symbols for market-cap probe",
    )
    parser.add_argument("--trading-days", type=int, default=320)
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="Read OHLCV only from stock_hist_cache for the market-cap probe.",
    )
    args = parser.parse_args()

    end_day = _date_arg(args.end, date.today())
    start_day = _date_arg(args.start, end_day - timedelta(days=90))
    start_s = start_day.strftime("%Y%m%d")
    end_s = end_day.strftime("%Y%m%d")
    index_code = str(args.index_code).strip()
    yf_index = _cn_index_to_yfinance_symbol(index_code)

    probes: list[dict[str, Any]] = []
    probes.append(
        _run_probe(
            f"fetch_index_hist({index_code})",
            lambda: fetch_index_hist(index_code, start_s, end_s, market="cn"),
        )
    )
    probes.append(
        _run_probe(
            f"yfinance({yf_index})",
            lambda: _fetch_index_yfinance(yf_index, start_s, end_s),
        )
    )
    probes.append(
        _run_probe(
            f"akshare.index_zh_a_hist({index_code})",
            lambda: _fetch_index_akshare(index_code, start_s, end_s),
        )
    )

    symbols = [x.strip() for x in str(args.symbols or "").split(",") if x.strip()]
    cached_cap_map = fetch_market_cap_map(market="cn")
    window = _resolve_trading_window(
        end_calendar_day=end_day,
        trading_days=max(int(args.trading_days), 30),
    )
    df_map: dict[str, pd.DataFrame] = {}
    hist_errors: dict[str, str] = {}
    for symbol in symbols:
        try:
            raw_df = get_stock_hist(
                symbol=symbol,
                start_date=window.start_trade_date,
                end_date=window.end_trade_date,
                adjust="qfq",
                market="cn",
                context="background",
                cache_only=bool(args.cache_only),
            )
            df_map[symbol] = normalize_hist_from_fetch(raw_df)
        except Exception as exc:
            hist_errors[symbol] = f"{type(exc).__name__}: {exc}"

    enriched_cap_map, cap_stats = build_market_cap_map_from_shares(
        symbols=symbols,
        market="cn",
        df_map=df_map,
        base_map=cached_cap_map,
        refresh_missing=True,
    )
    probes.append(
        {
            "name": "market_cap",
            "ok": True,
            "summary": {
                "cached_count": len(cached_cap_map),
                "enriched_count": len(enriched_cap_map),
                "sample_values": {
                    symbol: enriched_cap_map.get(symbol) for symbol in symbols
                },
                "hist_rows": {
                    symbol: int(len(df.index)) for symbol, df in df_map.items()
                },
                "hist_errors": hist_errors,
                "stats": cap_stats,
            },
        }
    )

    payload = {
        "range": {"start": start_day.isoformat(), "end": end_day.isoformat()},
        "index_code": index_code,
        "yfinance_index_symbol": yf_index,
        "cache_only": bool(args.cache_only),
        "cwd": str(Path.cwd()),
        "probes": probes,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if all(item.get("ok") for item in probes[:1]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
