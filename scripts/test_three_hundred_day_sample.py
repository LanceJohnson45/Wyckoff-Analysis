# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
import sys

import pandas as pd
from dotenv import load_dotenv


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.indicator_rule_engine import load_indicator_definition
from core.stock_cache import get_cache_meta
from core.stock_cache import normalize_hist_df
from core.three_hundred_day_engine import DEFAULT_QUERY_DIR, scan_three_hundred_day_lazy
from integrations.fetch_a_share_csv import _resolve_trading_window
from integrations.stock_hist_repository import get_stock_hist
from tools.symbol_pool import resolve_symbol_pool_from_env
from utils.trading_clock import resolve_end_calendar_day_for_market

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SNAPSHOT_ROOT = PROJECT_ROOT / "data" / "funnel_snapshots"

load_dotenv(PROJECT_ROOT / ".env")


def _computable_specs_requirements(query_dir: Path) -> tuple[int, int]:
    spec_count = 0
    for path in sorted(query_dir.glob("*.json")):
        if path.name == "extraction_progress.json":
            continue
        definition = load_indicator_definition(path)
        if not definition.get("computable_from_daily_ocvhl", False):
            continue
        spec_count += 1
    return spec_count, 0


def _load_cached_daily_history(code: str, required_bars: int):
    end_day = resolve_end_calendar_day_for_market("cn")
    window = _resolve_trading_window(
        end_calendar_day=end_day,
        trading_days=max(required_bars, 30),
    )
    raw = get_stock_hist(
        code,
        window.start_trade_date,
        window.end_trade_date,
        adjust="qfq",
        market="cn",
        context="background",
        cache_only=True,
    )
    if raw is None or raw.empty:
        return None
    norm = normalize_hist_df(raw)
    if norm is None or norm.empty:
        return None
    return norm.reset_index(drop=True)


def _load_latest_snapshot_frames(snapshot_root: Path) -> tuple[str | None, dict[str, pd.DataFrame], dict[str, str]]:
    latest_run_file = snapshot_root / "latest_run.txt"
    if not latest_run_file.exists():
        return None, {}, {}
    run_dir = Path(latest_run_file.read_text(encoding="utf-8").strip())
    hist_path = run_dir / "hist_full.csv.gz"
    if not hist_path.exists():
        return str(run_dir), {}, {}
    full_df = pd.read_csv(hist_path, compression="gzip")
    if full_df.empty or "symbol" not in full_df.columns:
        return str(run_dir), {}, {}

    name_map: dict[str, str] = {}
    if "name" in full_df.columns:
        latest_names = (
            full_df[["symbol", "name"]]
            .dropna(subset=["symbol"])
            .drop_duplicates(subset=["symbol"], keep="last")
        )
        name_map = {
            str(row["symbol"]).strip(): str(row["name"]).strip()
            for _, row in latest_names.iterrows()
        }

    df_map: dict[str, pd.DataFrame] = {}
    for symbol, group in full_df.groupby("symbol"):
        code = str(symbol).strip()
        if not code:
            continue
        norm = normalize_hist_df(group)
        if norm is None or norm.empty:
            continue
        df_map[code] = norm.reset_index(drop=True)
    return str(run_dir), df_map, name_map


def main() -> int:
    parser = argparse.ArgumentParser(
        description="随机抽取 A 股样本，验证 300day 引擎在现有缓存上能否跑通。"
    )
    parser.add_argument("--sample-size", type=int, default=30, help="目标抽样股票数，默认 30")
    parser.add_argument("--seed", type=int, default=300, help="随机种子，默认 300")
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=120,
        help="最大尝试股票数，默认 120，用于应对个别缓存缺失",
    )
    parser.add_argument(
        "--output-dir",
        default="data/analysis_results/sample_runs",
        help="样本测试结果目录，默认 data/analysis_results/sample_runs",
    )
    parser.add_argument(
        "--snapshot-root",
        default=str(DEFAULT_SNAPSHOT_ROOT),
        help="Wyckoff 漏斗全量快照根目录，默认 data/funnel_snapshots",
    )
    args = parser.parse_args()

    query_dir = Path(DEFAULT_QUERY_DIR)
    output_root = Path(args.output_dir)
    spec_count, _ = _computable_specs_requirements(query_dir)
    symbols, name_map, pool_stats = resolve_symbol_pool_from_env()
    if not symbols:
        print("A股股票池为空，无法执行样本测试。", file=sys.stderr)
        return 2

    rng = random.Random(int(args.seed))
    shuffled = list(symbols)
    rng.shuffle(shuffled)
    candidates = shuffled[: max(int(args.max_attempts), int(args.sample_size))]

    df_map: dict[str, object] = {}
    sampled_codes: list[str] = []
    skipped_no_cache: list[str] = []
    skipped_no_cache_seen: set[str] = set()

    print(
        "[sample300] 准备开始："
        f" pool={len(symbols)}, spec_count={spec_count}, lazy_fetch=on, "
        f"sample_size={args.sample_size}, max_attempts={args.max_attempts}, seed={args.seed}"
    )
    print(f"[sample300] pool_stats={json.dumps(pool_stats, ensure_ascii=False)}")

    snapshot_run_dir, snapshot_df_map, snapshot_name_map = _load_latest_snapshot_frames(
        Path(args.snapshot_root)
    )
    if snapshot_df_map:
        print(
            f"[sample300] 使用最新漏斗快照: run_dir={snapshot_run_dir}, "
            f"symbols={len(snapshot_df_map)}"
        )
    sampled_codes = candidates[: int(args.sample_size)]
    if not sampled_codes:
        print("[sample300] 没有可用的抽样代码。", file=sys.stderr)
        return 2
    if snapshot_name_map:
        for code, stock_name in snapshot_name_map.items():
            if code not in name_map and stock_name:
                name_map[code] = stock_name

    fetch_stats = {
        "snapshot_hit": 0,
        "cache_hit": 0,
        "empty": 0,
        "no_cache_meta": 0,
    }
    local_frame_cache: dict[tuple[str, int], pd.DataFrame | None] = {}
    symbol_cache_presence: dict[str, bool] = {}

    def _history_loader(code: str, required_bars: int):
        cache_key = (code, int(required_bars))
        if cache_key in local_frame_cache:
            return local_frame_cache[cache_key]

        snapshot_df = snapshot_df_map.get(code)
        if snapshot_df is not None and not snapshot_df.empty and len(snapshot_df) >= int(required_bars):
            fetch_stats["snapshot_hit"] += 1
            frame = snapshot_df.tail(int(required_bars)).reset_index(drop=True)
            local_frame_cache[cache_key] = frame
            print(
                f"[sample300] load(snapshot,lazy) {code} {name_map.get(code, '')} "
                f"bars={required_bars} rows={len(frame)}"
            )
            return frame

        if code not in symbol_cache_presence:
            meta = get_cache_meta(code, "qfq", context="background")
            symbol_cache_presence[code] = meta is not None
            if meta is None:
                fetch_stats["no_cache_meta"] += 1
                print(f"[sample300] no_cache_meta {code} {name_map.get(code, '')}")
        if not symbol_cache_presence.get(code, False):
            fetch_stats["empty"] += 1
            skipped_key = f"{code}:no_cache_meta"
            if skipped_key not in skipped_no_cache_seen:
                skipped_no_cache_seen.add(skipped_key)
                skipped_no_cache.append(skipped_key)
            local_frame_cache[cache_key] = None
            return None

        frame = _load_cached_daily_history(code, int(required_bars))
        if frame is not None and not frame.empty and len(frame) >= int(required_bars):
            fetch_stats["cache_hit"] += 1
            frame = frame.tail(int(required_bars)).reset_index(drop=True)
            local_frame_cache[cache_key] = frame
            print(
                f"[sample300] load(cache,lazy) {code} {name_map.get(code, '')} "
                f"bars={required_bars} rows={len(frame)}"
            )
            return frame

        fetch_stats["empty"] += 1
        skipped_key = f"{code}:{required_bars}"
        if skipped_key not in skipped_no_cache_seen:
            skipped_no_cache_seen.add(skipped_key)
            skipped_no_cache.append(skipped_key)
        local_frame_cache[cache_key] = None
        return None

    trade_date = str(resolve_end_calendar_day_for_market("cn"))
    run_output_dir = output_root / trade_date / f"sample_{len(sampled_codes)}_seed_{args.seed}"
    result = scan_three_hundred_day_lazy(
        symbols=sampled_codes,
        name_map=name_map,
        trade_date=trade_date,
        history_loader=_history_loader,
        market="cn",
        query_dir=query_dir,
        output_dir=run_output_dir,
    )

    print(
        "[sample300] 扫描完成："
        f" scanned={result.scanned_symbols}, matched_symbols={result.matched_symbols}, "
        f"matched_indicators={result.matched_indicator_total}"
    )
    print(f"[sample300] report={result.output_path}")
    print(f"[sample300] sampled_codes={','.join(sampled_codes)}")
    if skipped_no_cache:
        print(f"[sample300] skipped_no_cache={','.join(skipped_no_cache[:20])}")
    for item in result.symbols[:10]:
        hits = "；".join(f"{hit.indicator}" for hit in item.indicators[:5])
        print(f"[sample300] hit {item.code} {item.name} count={item.hit_count} indicators={hits}")

    meta = {
        "trade_date": trade_date,
        "sample_size_requested": int(args.sample_size),
        "sample_size_loaded": len(sampled_codes),
        "seed": int(args.seed),
        "max_attempts": int(args.max_attempts),
        "computable_spec_count": spec_count,
        "snapshot_run_dir": snapshot_run_dir,
        "sampled_codes": sampled_codes,
        "skipped_no_cache": skipped_no_cache,
        "fetch_stats": fetch_stats,
        "report_path": result.output_path,
        "matched_symbols": result.matched_symbols,
        "matched_indicator_total": result.matched_indicator_total,
    }
    meta_path = run_output_dir / "sample_meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[sample300] meta={meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
