# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import sys

from core.indicator_rule_engine import (
    IndicatorRuleEngine,
    infer_data_requirements,
    load_indicator_definition,
)
from core.stock_cache import normalize_hist_df
from integrations.data_source import fetch_stock_hist
from integrations.stock_hist_repository import get_stock_hist


QUERY_DIR = Path("/Volumes/E/github/Wyckoff-Analysis/query")


@dataclass(frozen=True)
class SymbolTarget:
    symbol: str
    market: str


def _indicator_paths(spec_names: list[str]) -> list[Path]:
    if spec_names:
        paths = []
        for name in spec_names:
            path = QUERY_DIR / f"{name}.json"
            if not path.exists():
                raise FileNotFoundError(f"找不到指标定义: {path}")
            paths.append(path)
        return paths
    return sorted(
        path
        for path in QUERY_DIR.glob("*.json")
        if path.name != "extraction_progress.json"
    )


def _load_real_history(target: SymbolTarget, bars: int, cache_only: bool):
    end = date.today()
    start = end - timedelta(days=max(int(bars * 2.2), 120))
    raw = None
    if cache_only:
        raw = get_stock_hist(
            target.symbol,
            start,
            end,
            adjust="qfq",
            market=target.market,
            context="background",
            cache_only=True,
        )
    else:
        try:
            raw = get_stock_hist(
                target.symbol,
                start,
                end,
                adjust="qfq",
                market=target.market,
                context="background",
                cache_only=False,
            )
        except Exception:
            raw = fetch_stock_hist(
                symbol=target.symbol,
                start=start,
                end=end,
                adjust="qfq",
                market=target.market,
            )
    if raw is None or raw.empty:
        return None
    norm = normalize_hist_df(raw)
    if norm is None or norm.empty:
        return None
    return norm.tail(bars).reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="传入 market + code + spec，由脚本自动推导规则所需数据并完成真实日线校验"
    )
    parser.add_argument(
        "--market",
        required=True,
        choices=["cn", "us", "hk"],
        help="标的市场",
    )
    parser.add_argument(
        "--code",
        required=True,
        help="标的代码，例如 000725、AAPL、00700",
    )
    parser.add_argument(
        "--spec",
        required=True,
        help="指标文件名（不带 .json），例如 jiatu",
    )
    parser.add_argument(
        "--bars",
        type=int,
        default=0,
        help="可选覆盖值；若大于 0，则强制使用该日线根数，否则按规则定义自动推导",
    )
    parser.add_argument(
        "--evaluation-buffer",
        type=int,
        default=20,
        help="在 warmup_bars 之外额外多取的日线根数，用于 latest_match 回放，默认 20",
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help="只用项目已有缓存，不去在线行情源补拉",
    )
    args = parser.parse_args()

    target = SymbolTarget(symbol=str(args.code).strip(), market=str(args.market).strip().lower())
    specs = _indicator_paths([args.spec])
    engine = IndicatorRuleEngine()
    spec_defs = {path: load_indicator_definition(path) for path in specs}
    spec_requirements = {
        path: infer_data_requirements(defn, evaluation_buffer=args.evaluation_buffer)
        for path, defn in spec_defs.items()
    }
    required_bars = max(
        args.bars if args.bars > 0 else 0,
        max(req.min_bars for req in spec_requirements.values()),
    )

    failures: list[str] = []

    try:
        df = _load_real_history(target, required_bars, cache_only=args.cache_only)
    except Exception as exc:
        print(
            f"[DATA ERROR] {target.symbol}:{target.market} -> {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2

    if df is None or df.empty:
        print("未加载到任何真实数据样本。", file=sys.stderr)
        print(
            f"[DATA EMPTY] {target.symbol}:{target.market} -> 未取到可用日线数据",
            file=sys.stderr,
        )
        return 2

    print(
        f"[TARGET] {target.symbol}:{target.market} rows={len(df)} "
        f"range={df.iloc[0]['date']}..{df.iloc[-1]['date']} required_bars={required_bars}"
    )
    for spec_path in specs:
        spec = spec_defs[spec_path]
        req = spec_requirements[spec_path]
        try:
            result = engine.evaluate(df, spec)
        except Exception as exc:
            failures.append(
                f"[RULE ERROR] {spec_path.stem} on {target.symbol}:{target.market} -> "
                f"{type(exc).__name__}: {exc}"
            )
            continue

        print(
            f"  - {spec_path.stem}: latest_match={result.latest_match} "
            f"signal_date={result.signal_date} required_fields_ok={result.required_fields_ok} "
            f"required_fields={list(req.required_fields)} warmup_bars={req.warmup_bars}"
        )

    if failures:
        print("\n校验失败列表：", file=sys.stderr)
        for line in failures:
            print(line, file=sys.stderr)
        return 1

    print("\n真实数据规则校验通过。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
