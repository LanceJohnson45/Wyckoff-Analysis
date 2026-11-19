"""固定美股维护目标日，开跑前确认 Yahoo 已提供有效收盘数据。"""

import argparse
from datetime import date, timedelta
import os

import pandas as pd
import structlog

from integrations.data_source import _fetch_stock_yfinance
from scripts.us_yfinance_backfill import _approx_us_market_dates
from utils.trading_clock import resolve_end_calendar_day_for_market

LOGGER = structlog.get_logger()


def verify_target(target: date) -> None:
    """拒绝仅有成交量而无 OHLC 的目标日，避免全池空跑。"""
    for symbol in ("AAPL", "SPY"):
        frame = _fetch_stock_yfinance(symbol, target.strftime("%Y%m%d"), target.strftime("%Y%m%d"))
        prices = frame[["开盘", "最高", "最低", "收盘"]].apply(pd.to_numeric, errors="coerce")
        if frame.empty or not prices.notna().all(axis=1).any() or not prices.gt(0).all(axis=1).any():
            raise RuntimeError(f"Yahoo {symbol} {target} 未提供有效收盘 OHLC，上游日线未就绪，请稍后按同一日期重跑")


def main() -> int:
    """手动补跑可指定目标日期，自动任务保持市场时区的已收盘日期。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-date", default="")
    args = parser.parse_args()
    latest = resolve_end_calendar_day_for_market("us")
    requested = date.fromisoformat(args.end_date) if args.end_date else latest
    if requested > latest:
        parser.error("目标日不得晚于美股已收盘日期")
    target = _approx_us_market_dates(start=requested - timedelta(days=10), end=requested)[-1]
    if output := os.getenv("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            stream.write(f"target_date={target}\n")
    verify_target(target)
    LOGGER.info("us_market_target_ready", target_date=str(target))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
