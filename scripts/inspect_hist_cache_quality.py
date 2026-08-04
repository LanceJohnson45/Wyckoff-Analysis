#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Inspect cached OHLCV rows and quality issues for selected symbols."""

from __future__ import annotations

import argparse
from datetime import date, timedelta
import os
import sys

import pandas as pd

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.kline_quality import check_kline_quality
from core.stock_cache import get_cache_meta, load_cached_history, normalize_hist_df
from core.wyckoff_engine import normalize_hist_from_fetch


DEFAULT_SYMBOLS = ("000004", "000060", "000063", "000065", "000096")


def _parse_symbols(values: list[str]) -> list[str]:
    out: list[str] = []
    for value in values:
        for part in str(value or "").replace("，", ",").split(","):
            symbol = part.strip()
            if symbol:
                out.append(symbol)
    return out


def _cache_symbol(symbol: str, market: str) -> str:
    market_norm = str(market or "cn").strip().lower()
    if market_norm == "us":
        return f"US:{symbol}"
    if market_norm == "hk":
        return f"HK:{symbol}"
    return symbol


def _date_arg(value: str | None) -> date | None:
    if not value:
        return None
    return pd.to_datetime(str(value), errors="raise").date()


def _missing_counts(df: pd.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for col in ("open", "high", "low", "close", "volume", "amount", "pct_chg"):
        if col not in df.columns:
            counts[col] = -1
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if col in {"volume", "amount"}:
            counts[col] = int((s.isna() | (s <= 0)).sum())
        else:
            counts[col] = int(s.isna().sum())
    return counts


def _issue_text(df: pd.DataFrame, symbol: str) -> str:
    report = check_kline_quality(df, symbol=symbol)
    if not report.issues:
        return "无"
    return "、".join(
        f"{issue.severity}:{issue.category}={max(issue.count, 1)}"
        for issue in report.issues
    )


def _sample_bad_rows(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if df.empty:
        return df
    masks = []
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if col in {"volume", "amount"}:
            masks.append(s.isna() | (s <= 0))
        else:
            masks.append(s.isna())
    if not masks:
        return pd.DataFrame()
    mask = masks[0]
    for item in masks[1:]:
        mask = mask | item
    cols = [c for c in ("date", "open", "high", "low", "close", "volume", "amount", "pct_chg") if c in df.columns]
    return df.loc[mask, cols].tail(limit)


def _print_frame(label: str, df: pd.DataFrame) -> None:
    if df.empty:
        print(f"{label}: 无")
        return
    print(label + ":")
    print(df.to_string(index=False))


def inspect_symbol(
    symbol: str,
    *,
    market: str,
    adjust: str,
    start: date | None,
    end: date,
    days: int,
    context: str,
    bad_rows: int,
) -> None:
    cache_sym = _cache_symbol(symbol, market)
    meta = get_cache_meta(cache_sym, adjust, context=context)
    print("=" * 88)
    print(f"symbol={symbol} cache_symbol={cache_sym} market={market} adjust={adjust}")
    if meta is None:
        print("cache_meta: 未找到")
        return

    query_start = start or max(meta.start_date, end - timedelta(days=max(days, 1)))
    query_end = min(end, meta.end_date)
    print(
        "cache_meta: "
        f"range={meta.start_date}..{meta.end_date} updated_at={meta.updated_at} "
        f"query={query_start}..{query_end}"
    )
    raw = load_cached_history(
        cache_sym,
        adjust,
        meta.source,
        query_start,
        query_end,
        context=context,
    )
    if raw is None or raw.empty:
        print("cached_rows: 0")
        return

    raw = raw.sort_values("date").reset_index(drop=True)
    normalized = normalize_hist_df(raw)
    funnel_norm = normalize_hist_from_fetch(raw)

    print(f"cached_rows: {len(raw)}")
    print(f"raw_missing: {_missing_counts(raw)}")
    print(f"cache_normalized_missing: {_missing_counts(normalized)}")
    print(f"funnel_normalized_missing: {_missing_counts(funnel_norm)}")
    print(f"raw_quality: {_issue_text(raw, symbol)}")
    print(f"funnel_quality: {_issue_text(funnel_norm, symbol)}")
    _print_frame("raw_bad_rows_tail", _sample_bad_rows(raw, bad_rows))
    _print_frame("funnel_bad_rows_tail", _sample_bad_rows(funnel_norm, bad_rows))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect stock_hist_cache OHLCV data and quality issues.",
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Symbols to inspect, comma separated or space separated.",
    )
    parser.add_argument("--market", choices=("cn", "us", "hk"), default="cn")
    parser.add_argument("--adjust", default="qfq")
    parser.add_argument("--start", default="")
    parser.add_argument("--end", default=date.today().isoformat())
    parser.add_argument("--days", type=int, default=420)
    parser.add_argument("--context", default="background")
    parser.add_argument("--bad-rows", type=int, default=12)
    args = parser.parse_args()

    symbols = _parse_symbols(args.symbols) or list(DEFAULT_SYMBOLS)
    start = _date_arg(args.start)
    end = _date_arg(args.end) or date.today()
    for symbol in symbols:
        inspect_symbol(
            symbol,
            market=args.market,
            adjust=args.adjust,
            start=start,
            end=end,
            days=args.days,
            context=args.context,
            bad_rows=max(args.bad_rows, 1),
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
