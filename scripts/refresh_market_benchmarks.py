"""Actions数据任务：维护沪深300/SPY完整日线，供本机一并同步。"""

from __future__ import annotations

import argparse
from datetime import timedelta
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

BENCHMARK_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(BENCHMARK_SCRIPT_ROOT))
load_dotenv(BENCHMARK_SCRIPT_ROOT / ".env")

from core.kline_quality import check_kline_quality
from core.stock_cache import normalize_hist_df, upsert_cache_data
from integrations.data_source import fetch_index_hist
from integrations.fetch_a_share_csv import _resolve_trading_window
from integrations.market_data_sync import BENCHMARK_SYMBOLS
from utils.trading_clock import resolve_end_calendar_day_for_market


def refresh_benchmark(market: str, *, calendar_days: int = 550) -> int:
    """覆盖重取小规模基准历史，保留既有数据源的调整口径。"""
    if market not in BENCHMARK_SYMBOLS or calendar_days < 200:
        raise ValueError("市场必须为 cn/us，历史窗口至少200个自然日")
    symbol = BENCHMARK_SYMBOLS[market]
    end = resolve_end_calendar_day_for_market(market)
    required_end = _resolve_trading_window(end, 1).end_trade_date if market == "cn" else None
    frame = normalize_hist_df(fetch_index_hist(symbol, end - timedelta(days=calendar_days), end, market=market, min_rows=100, required_end=required_end))
    report = check_kline_quality(frame)
    if not report.ok or len(frame) < 100:
        raise ValueError("基准历史不足或质量检查未通过")
    if not upsert_cache_data(f"INDEX:{market.upper()}:{symbol}", "qfq", "market_benchmark", frame, context="background"):
        raise RuntimeError("基准写入云端失败")
    return len(frame)


def main() -> int:
    """数据任务入口，不执行任何形态或交易策略。"""
    parser = argparse.ArgumentParser(description="更新本机分析使用的云端基准日线")
    parser.add_argument("--market", required=True, choices=["cn", "us"])
    args = parser.parse_args()
    rows = refresh_benchmark(args.market)
    sys.stdout.write(json.dumps({"market": args.market, "benchmark_rows": rows}, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
