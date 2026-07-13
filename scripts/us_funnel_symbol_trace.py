#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trace the US Wyckoff funnel for one symbol.

Examples:
  python scripts/us_funnel_symbol_trace.py AAPL
  python scripts/us_funnel_symbol_trace.py NVDA --rps-universe sp500 --max-rps-symbols 120
  python scripts/us_funnel_symbol_trace.py TSLA --json-out /tmp/tsla_funnel_trace.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from core.kline_quality import check_kline_quality_map, summarize_quality_reports
from core.sector_rotation import analyze_sector_rotation
from core.wyckoff_engine import (
    DataIntegrityPolicy,
    FunnelConfig,
    _market_allowed_tracks,
    _resolve_l2_market,
    build_feature_map,
    detect_accum_stage,
    detect_markup_stage,
    filter_symbols_by_integrity,
    layer1_filter,
    layer2_strength_detailed,
    layer3_sector_resonance,
    layer4_triggers,
    layer5_exit_signals,
)
from integrations.data_source import fetch_index_hist
from integrations.fetch_a_share_csv import _fetch_hist_with_market, _resolve_us_window
from integrations.us_sp500_universe import get_sp500_constituents
from scripts.wyckoff_funnel import (
    US_MAIN_BENCH_CODE,
    US_SMALLCAP_BENCH_CODE,
    _analyze_benchmark_and_tune_cfg,
    _calc_market_breadth,
)
from tools.candidate_ranker import rank_l3_candidates
from tools.funnel_config import apply_funnel_cfg_overrides


def _line(title: str = "") -> None:
    if title:
        print(f"\n{'=' * 24} {title} {'=' * 24}")
    else:
        print("=" * 72)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [_jsonable(v) for v in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _pct(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.2f}%"


def _fmt_bool(value: bool) -> str:
    return "PASS" if value else "FAIL"


def _latest_date(df: pd.DataFrame | None) -> str:
    if df is None or df.empty or "date" not in df.columns:
        return "N/A"
    s = pd.to_datetime(df["date"], errors="coerce").dropna()
    return "N/A" if s.empty else s.iloc[-1].date().isoformat()


def _print_df_snapshot(symbol: str, df: pd.DataFrame) -> None:
    _log(
        f"{symbol} data rows={len(df)}, latest={_latest_date(df)}, "
        f"cols={list(df.columns)}"
    )
    if df.empty:
        return
    cols = [c for c in ["date", "open", "high", "low", "close", "volume", "amount", "pct_chg"] if c in df.columns]
    tail = df[cols].tail(8).copy()
    print(tail.to_string(index=False))


def _fetch_symbol(symbol: str, window, *, label: str) -> pd.DataFrame:
    started = time.monotonic()
    _log(f"fetch {label} {symbol} range={window.start_trade_date}..{window.end_trade_date}")
    df = _fetch_hist_with_market(symbol, window, "qfq", "us")
    elapsed = time.monotonic() - started
    _log(
        f"fetch ok {label} {symbol} rows={len(df)}, latest={_latest_date(df)}, "
        f"elapsed={elapsed:.2f}s, source={df.attrs.get('source')}"
    )
    return df


def _load_rps_symbols(symbol: str, mode: str, max_symbols: int) -> list[str]:
    symbol = symbol.upper().strip()
    mode = str(mode or "single").strip().lower()
    if mode == "none":
        return [symbol]
    if mode == "single":
        return [symbol]
    if mode != "sp500":
        raise ValueError("--rps-universe must be one of: single, sp500, none")

    snapshot = get_sp500_constituents(prefer_snapshot=True)
    symbols = [s.upper() for s in snapshot.symbols if s]
    if symbol not in symbols:
        symbols.insert(0, symbol)
    else:
        symbols = [symbol] + [s for s in symbols if s != symbol]
    if max_symbols > 0:
        symbols = symbols[: max_symbols]
        if symbol not in symbols:
            symbols.insert(0, symbol)
    _log(
        f"rps universe sp500 source={snapshot.source}, as_of={snapshot.as_of}, "
        f"symbols={len(symbols)}"
    )
    return symbols


def _print_l1(symbol: str, passed: list[str], rejected: dict[str, dict]) -> None:
    _line("Layer 1")
    if symbol in passed:
        print(f"L1 result: PASS ({symbol} entered Layer 2)")
    else:
        print(f"L1 result: FAIL reason={rejected.get(symbol)}")


def _print_l2(symbol: str, channel_map: dict[str, str], rejected: dict[str, dict], decisions: dict) -> None:
    _line("Layer 2")
    decision = decisions.get(symbol)
    if symbol in channel_map:
        print(f"L2 result: PASS channel={channel_map.get(symbol)}")
    else:
        print(f"L2 result: FAIL rejection={json.dumps(_jsonable(rejected.get(symbol)), ensure_ascii=False, indent=2)}")
    if decision is None:
        print("L2 decision: N/A")
        return
    print(
        "L2 selected: "
        f"passed={decision.passed}, track={decision.selected_track}, score={decision.selected_score}"
    )
    print(f"L2 track scores: {json.dumps(_jsonable(decision.track_scores), ensure_ascii=False)}")
    details = (decision.reasons or {}).get("track_details") or {}
    for track, detail in details.items():
        print(f"\nTrack {track}:")
        print(json.dumps(_jsonable(detail), ensure_ascii=False, indent=2))


def _print_l3_l4(
    symbol: str,
    l3_passed: list[str],
    top_sectors: list[str],
    triggers: dict[str, list[tuple[str, float]]],
    stage_map: dict[str, str],
    markup_symbols: list[str],
    exit_signals: dict[str, dict],
    score_map: dict[str, float],
) -> None:
    _line("Layer 3")
    print(f"L3 result: {_fmt_bool(symbol in l3_passed)} top_sectors={top_sectors}")
    print(f"L3 rank score: {score_map.get(symbol)}")

    _line("Layer 4 / Stage / Exit")
    symbol_triggers = {
        name: score
        for name, rows in triggers.items()
        for code, score in rows
        if str(code).upper() == symbol.upper()
    }
    print(f"L4 triggers: {json.dumps(_jsonable(symbol_triggers), ensure_ascii=False)}")
    print(f"Markup: {_fmt_bool(symbol in markup_symbols)}")
    print(f"Accum stage: {stage_map.get(symbol) or 'N/A'}")
    print(f"Exit signal: {json.dumps(_jsonable(exit_signals.get(symbol)), ensure_ascii=False)}")


def _print_feature_summary(symbol: str, explanation: dict | None, df: pd.DataFrame) -> None:
    _line("Computed Values")
    close = pd.to_numeric(df.get("close"), errors="coerce")
    volume = pd.to_numeric(df.get("volume"), errors="coerce")
    latest_close = float(close.iloc[-1]) if not close.empty and pd.notna(close.iloc[-1]) else None
    ma20 = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else None
    ma50 = close.rolling(50).mean().iloc[-1] if len(close) >= 50 else None
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
    ret5 = ((close.iloc[-1] - close.iloc[-6]) / close.iloc[-6] * 100.0) if len(close) > 5 else None
    ret20 = ((close.iloc[-1] - close.iloc[-21]) / close.iloc[-21] * 100.0) if len(close) > 20 else None
    vol20 = volume.tail(20).mean() if len(volume) >= 20 else None
    vol60 = volume.tail(60).mean() if len(volume) >= 60 else None
    vol_ratio = float(vol20 / vol60) if vol20 is not None and vol60 not in (None, 0) else None
    print(f"symbol={symbol}")
    print(f"latest_close={latest_close}")
    print(f"ma20={ma20}, ma50={ma50}, ma200={ma200}")
    print(f"ret5={_pct(ret5)}, ret20={_pct(ret20)}, vol20/vol60={vol_ratio}")
    if explanation:
        print("\nEngine explanation:")
        print(json.dumps(_jsonable(explanation), ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Trace one US symbol through the Wyckoff funnel")
    parser.add_argument("symbol", help="US ticker, e.g. AAPL")
    parser.add_argument("--trading-days", type=int, default=320, help="Trading-day window, default 320")
    parser.add_argument("--end-date", default="", help="End calendar day YYYY-MM-DD, default today")
    parser.add_argument(
        "--rps-universe",
        default="single",
        choices=["single", "sp500", "none"],
        help="RPS universe. 'single' is fast but ranks the symbol against itself; 'sp500' is closer to real funnel.",
    )
    parser.add_argument("--max-rps-symbols", type=int, default=80, help="Max symbols to fetch for --rps-universe sp500; 0 means all")
    parser.add_argument("--json-out", default="", help="Optional path to write full trace JSON")
    args = parser.parse_args()

    symbol = args.symbol.strip().upper()
    if not symbol:
        raise SystemExit("symbol is empty")

    os.environ["FUNNEL_MARKET"] = "us"
    os.environ.setdefault("DATA_SOURCE_DEBUG", "1")

    end_day = date.fromisoformat(args.end_date) if args.end_date else date.today()
    window = _resolve_us_window(end_calendar_day=end_day, trading_days=args.trading_days)
    cfg = FunnelConfig.for_market("us")
    cfg.trading_days = int(args.trading_days)
    apply_funnel_cfg_overrides(cfg)

    l2_market = _resolve_l2_market(cfg)
    allowed_tracks = _market_allowed_tracks(cfg, l2_market)

    _line("Setup")
    print(f"symbol={symbol}")
    print(f"window={window.start_trade_date}..{window.end_trade_date}, trading_days={args.trading_days}")
    print(f"env FUNNEL_MARKET={os.getenv('FUNNEL_MARKET')}")
    print(f"cfg profile={cfg.profile}, market_template={cfg.market_template}, style={cfg.style_template}")
    print(f"resolved_l2_market={l2_market}, allowed_tracks={list(allowed_tracks)}")
    print(
        "thresholds "
        f"L1_amount={cfg.min_avg_amount_wan}wan, "
        f"rps_fast={cfg.rps_fast_min}, rps_slow={cfg.rps_slow_min}, "
        f"trackA_min={cfg.track_a_min_score}, trackB_min={cfg.track_b_min_score}, "
        f"trackA_rps=({cfg.track_a_rps_fast_min},{cfg.track_a_rps_slow_min}), "
        f"trackB_rps={cfg.track_b_rps_fast_min}"
    )

    _line("Fetch Data")
    target_df = _fetch_symbol(symbol, window, label="target")
    bench_df = fetch_index_hist(US_MAIN_BENCH_CODE, window.start_trade_date, window.end_trade_date, market="us")
    smallcap_df = fetch_index_hist(US_SMALLCAP_BENCH_CODE, window.start_trade_date, window.end_trade_date, market="us")
    _log(f"benchmark {US_MAIN_BENCH_CODE} rows={len(bench_df)}, latest={_latest_date(bench_df)}")
    _log(f"smallcap {US_SMALLCAP_BENCH_CODE} rows={len(smallcap_df)}, latest={_latest_date(smallcap_df)}")
    _print_df_snapshot(symbol, target_df)

    rps_symbols = _load_rps_symbols(symbol, args.rps_universe, args.max_rps_symbols)
    df_map: dict[str, pd.DataFrame] = {symbol: target_df}
    fetch_failures: dict[str, str] = {}
    if args.rps_universe == "sp500":
        _line("Fetch RPS Universe")
        for idx, sym in enumerate(rps_symbols, start=1):
            if sym == symbol:
                continue
            try:
                df_map[sym] = _fetch_symbol(sym, window, label=f"rps {idx}/{len(rps_symbols)}")
            except Exception as exc:
                fetch_failures[sym] = f"{type(exc).__name__}: {exc}"
                _log(f"fetch fail rps {sym}: {fetch_failures[sym]}")
        _log(f"rps fetch done ok={len(df_map)} fail={len(fetch_failures)}")

    _line("Benchmark Regime")
    breadth = _calc_market_breadth(df_map, ma_window=20, prev_offset=1)
    benchmark_context = _analyze_benchmark_and_tune_cfg(bench_df, smallcap_df, cfg, breadth=breadth)
    print(json.dumps(_jsonable(benchmark_context), ensure_ascii=False, indent=2))

    _line("Integrity / Quality")
    expected_dates = list(pd.bdate_range(start=window.start_trade_date, end=window.end_trade_date).date)
    integrity_df_map, integrity_rejections = filter_symbols_by_integrity(
        df_map,
        expected_dates,
        policy=DataIntegrityPolicy(),
    )
    print(f"integrity target: {_fmt_bool(symbol in integrity_df_map)} rejection={integrity_rejections.get(symbol)}")
    quality_reports = check_kline_quality_map({symbol: target_df})
    quality_summary = summarize_quality_reports(quality_reports)
    print(f"quality summary: {json.dumps(_jsonable(quality_summary), ensure_ascii=False, indent=2)}")
    if symbol in quality_reports:
        print(f"quality target: {json.dumps(_jsonable(quality_reports[symbol]), ensure_ascii=False, indent=2)}")

    prepared_df_map = {sym: df.sort_values("date") for sym, df in integrity_df_map.items() if df is not None and not df.empty}
    feature_map = build_feature_map(prepared_df_map, cfg)
    name_map = {sym: sym for sym in prepared_df_map}
    market_cap_map: dict[str, float] = {}
    sector_map = {sym: "US_SINGLE_TRACE" for sym in prepared_df_map}

    l1_passed, l1_rejections = layer1_filter(
        [symbol],
        name_map,
        market_cap_map,
        prepared_df_map,
        cfg,
        market="us",
        feature_map=feature_map,
        return_rejections=True,
    )
    _print_l1(symbol, l1_passed, l1_rejections)

    l2_passed, l2_channel_map, l2_rejections, l2_decisions = layer2_strength_detailed(
        l1_passed,
        prepared_df_map,
        bench_df,
        cfg,
        rps_universe=[s for s in rps_symbols if s in prepared_df_map],
        feature_map=feature_map,
        return_rejections=True,
        return_decisions=True,
    )
    _print_l2(symbol, l2_channel_map, l2_rejections, l2_decisions)

    l3_passed, top_sectors = layer3_sector_resonance(
        l2_passed,
        sector_map,
        cfg,
        base_symbols=l1_passed,
        df_map=prepared_df_map,
    )
    sector_rotation = analyze_sector_rotation(
        prepared_df_map,
        sector_map,
        universe_symbols=list(prepared_df_map.keys()),
        focus_sectors=top_sectors,
    )
    triggers = layer4_triggers(
        l3_passed,
        prepared_df_map,
        cfg,
        channel_map=l2_channel_map,
        feature_map=feature_map,
    )
    markup_symbols = detect_markup_stage(l3_passed, prepared_df_map, cfg, feature_map=feature_map)
    stage_map = detect_accum_stage(l2_passed, prepared_df_map, cfg, feature_map=feature_map)
    exit_signals = layer5_exit_signals(
        l2_passed + markup_symbols,
        prepared_df_map,
        stage_map,
        cfg,
        feature_map=feature_map,
    )
    _ranked, score_map = rank_l3_candidates(
        l3_passed,
        prepared_df_map,
        sector_map,
        triggers,
        top_sectors,
        l2_channel_map=l2_channel_map,
        sector_rotation_map=(sector_rotation.get("state_map", {}) or {}),
    )
    _print_l3_l4(symbol, l3_passed, top_sectors, triggers, stage_map, markup_symbols, exit_signals, score_map)

    explanation = {
        "passed_layers": {
            "integrity": symbol in integrity_df_map,
            "layer1": symbol in l1_passed,
            "layer2": symbol in l2_passed,
            "layer3": symbol in l3_passed,
            "markup": symbol in markup_symbols,
        },
        "layer1_rejection": l1_rejections.get(symbol),
        "layer2_rejection": l2_rejections.get(symbol),
        "channel": l2_channel_map.get(symbol),
        "layer2_decision": (
            {
                "passed": l2_decisions[symbol].passed,
                "selected_track": l2_decisions[symbol].selected_track,
                "selected_score": l2_decisions[symbol].selected_score,
                "track_scores": l2_decisions[symbol].track_scores,
                "old_channels": l2_decisions[symbol].old_channels,
                "reasons": l2_decisions[symbol].reasons,
            }
            if symbol in l2_decisions
            else None
        ),
        "stage": stage_map.get(symbol),
        "triggers": {
            name: score
            for name, rows in triggers.items()
            for code, score in rows
            if str(code).upper() == symbol
        },
        "exit_signal": exit_signals.get(symbol),
    }
    _print_feature_summary(symbol, explanation, target_df)

    result = {
        "symbol": symbol,
        "window": {
            "start": window.start_trade_date.isoformat(),
            "end": window.end_trade_date.isoformat(),
            "trading_days": args.trading_days,
        },
        "cfg": {
            "profile": cfg.profile,
            "market_template": cfg.market_template,
            "style_template": cfg.style_template,
            "resolved_l2_market": l2_market,
            "allowed_tracks": list(allowed_tracks),
        },
        "fetch_failures": fetch_failures,
        "benchmark_context": benchmark_context,
        "quality_summary": quality_summary,
        "integrity_rejections": integrity_rejections,
        "layer1": {"passed": l1_passed, "rejections": l1_rejections},
        "layer2": {
            "passed": l2_passed,
            "channel_map": l2_channel_map,
            "rejections": l2_rejections,
            "decision": explanation["layer2_decision"],
        },
        "layer3": {"passed": l3_passed, "top_sectors": top_sectors, "score_map": score_map},
        "layer4": {"triggers": triggers},
        "stage": {"markup_symbols": markup_symbols, "stage_map": stage_map, "exit_signals": exit_signals},
        "explanation": explanation,
    }
    if args.json_out:
        out_path = Path(args.json_out).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2), encoding="utf-8")
        _log(f"json trace written: {out_path}")

    _line("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
