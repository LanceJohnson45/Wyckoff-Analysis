#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""批量扫描网格策略参数并比较收益。

示例：
  ./.venv/bin/python scripts/grid_strategy_parameter_sweep.py -d 10
  ./.venv/bin/python scripts/grid_strategy_parameter_sweep.py \
      --up-pcts 0.01,0.02,0.03 --down-pcts 0.01,0.02,0.03 \
      --shares 100,200,500
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from grid_strategy_backtest import (
    DEFAULT_DB_PATH,
    backtest_grid,
    load_or_fill_sina_cache,
)


def _parse_floats(raw: str) -> list[float]:
    values = [float(x.strip()) for x in raw.split(",") if x.strip()]
    if not values or any(x <= 0 for x in values):
        raise ValueError(f"参数列表必须是正数: {raw}")
    return values


def _parse_ints(raw: str) -> list[int]:
    values = [int(x.strip()) for x in raw.split(",") if x.strip()]
    if not values or any(x <= 0 for x in values):
        raise ValueError(f"股数列表必须是正整数: {raw}")
    return values


def _parse_days(raw: str) -> list[int]:
    values = sorted({int(x.strip()) for x in raw.split(",") if x.strip()})
    if not values or any(x <= 0 for x in values):
        raise ValueError(f"交易日周期列表必须是正整数: {raw}")
    return values


def _run_case(task: tuple[pd.Series, pd.Series, int, float, float, int, int, float, float, int, bool, bool, float]) -> dict[str, float | int | bool]:
    prices, timestamps, period_days, up_pct, down_pct, buy_qty, sell_qty, initial_cash, initial_shares, fee_rate, turn_buy, turn_sell, turn_pct = task
    result = backtest_grid(
        prices,
        up_pct=up_pct,
        down_pct=down_pct,
        sell_shares=sell_qty,
        buy_shares=buy_qty,
        initial_cash=initial_cash,
        initial_shares=initial_shares,
        fee_rate=fee_rate,
        timestamps=timestamps,
        enable_turn_buy=turn_buy,
        enable_turn_sell=turn_sell,
        turn_pct=turn_pct,
    )
    return {
        "days": period_days,
        "up_pct": up_pct,
        "down_pct": down_pct,
        "buy_shares": buy_qty,
        "sell_shares": sell_qty,
        "profit": result.profit,
        "return_pct": result.return_pct,
        "t_profit": result.t_profit,
        "t_pairs": result.t_pairs,
        "fees": result.fees,
        "buy_count": result.buy_count,
        "sell_count": result.sell_count,
        "final_shares": result.final_shares,
        "final_cash": result.final_cash,
        "final_position_value": result.final_shares * result.last_price,
        "final_asset": result.final_asset,
        "final_pnl": result.profit,
        "turn_buy": turn_buy,
        "turn_sell": turn_sell,
        "turn_pct": turn_pct,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="588060")
    parser.add_argument("-d", "--days", type=int, default=10)
    parser.add_argument("--days-list", default="", help="批量计算多个周期，例如 1,3,5,7,9；指定后覆盖 -d")
    parser.add_argument("--up-pcts", default="0.005,0.01,0.015,0.02,0.025,0.03")
    parser.add_argument("--down-pcts", default="0.005,0.01,0.015,0.02,0.025,0.03")
    parser.add_argument("--shares", default="100,200,500", help="同时作为买入和卖出股数")
    parser.add_argument("--buy-shares", default="", help="可选，覆盖 --shares 的买入股数列表")
    parser.add_argument("--sell-shares", default="", help="可选，覆盖 --shares 的卖出股数列表")
    parser.add_argument("--initial-cash", type=float, default=300000.0)
    parser.add_argument("--initial-shares", type=int, default=20000)
    parser.add_argument("--fee-rate", type=float, default=0.0001)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--output", type=Path, default=Path("data/grid_parameter_sweep.csv"))
    parser.add_argument("--workers", type=int, default=1, help="并行线程数，默认 1；例如 --workers 8")
    parser.add_argument("--enable-turn-buy", action="store_true", help="开启拐点确认买入")
    parser.add_argument("--enable-turn-sell", action="store_true", help="开启拐点确认卖出")
    parser.add_argument("--turn-pcts", default="0.005,0.01", help="拐点比例列表，例如 0.005,0.01")
    args = parser.parse_args()
    if args.workers <= 0:
        raise ValueError("--workers 必须大于 0")

    days_list = _parse_days(args.days_list) if args.days_list else [args.days]
    up_pcts = _parse_floats(args.up_pcts)
    down_pcts = _parse_floats(args.down_pcts)
    default_shares = _parse_ints(args.shares)
    buy_shares = _parse_ints(args.buy_shares) if args.buy_shares else default_shares
    sell_shares = _parse_ints(args.sell_shares) if args.sell_shares else default_shares
    turn_pcts = _parse_floats(args.turn_pcts) if (args.enable_turn_buy or args.enable_turn_sell) else [0.005]

    rows: list[dict[str, float | int]] = []
    total = len(days_list) * len(up_pcts) * len(down_pcts) * len(buy_shares) * len(sell_shares) * len(turn_pcts)
    for period_days in days_list:
        frame, fetched_rows, cached_days = load_or_fill_sina_cache(
            args.symbol, "1", period_days, args.db_path
        )
        if frame.empty:
            raise RuntimeError(f"本地没有可用的 1 分钟行情: days={period_days}")
        frame = frame.sort_values("bar_time").reset_index(drop=True)
        frame = frame.dropna(subset=["close"]).reset_index(drop=True)
        prices = frame["close"]
        timestamps = frame["bar_time"]
        print(f"周期 {period_days} 天: {len(prices)} 根 1 分钟 K 线，覆盖 {len(cached_days)} 个交易日，新增写入 {fetched_rows} 条")
        tasks = [
            (prices, timestamps, period_days, up_pct, down_pct, buy_qty, sell_qty,
             args.initial_cash, args.initial_shares, args.fee_rate,
             args.enable_turn_buy, args.enable_turn_sell, turn_pct)
            for up_pct in up_pcts
            for down_pct in down_pcts
            for buy_qty in buy_shares
            for sell_qty in sell_shares
            for turn_pct in turn_pcts
        ]
        if args.workers == 1:
            rows.extend(_run_case(task) for task in tasks)
        else:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                rows.extend(executor.map(_run_case, tasks))

    result_df = pd.DataFrame(rows).sort_values(
        ["profit", "t_profit"], ascending=False
    ).reset_index(drop=True)
    result_df = result_df.rename(
        columns={
            "days": "周期(交易日)",
            "up_pct": "上涨比例",
            "down_pct": "下跌比例",
            "buy_shares": "买入股数",
            "sell_shares": "卖出股数",
            "profit": "总收益(元)",
            "return_pct": "收益率(%)",
            "t_profit": "做T收益(元)",
            "t_pairs": "做T配对笔数",
            "fees": "交易成本(元)",
            "buy_count": "买入次数",
            "sell_count": "卖出次数",
            "final_shares": "期末持仓(股)",
            "final_cash": "期末现金(元)",
            "final_position_value": "期末持仓市值(元)",
            "final_asset": "期末总资产(元)",
            "final_pnl": "最后总盈亏(元)",
            "turn_buy": "开启拐点买入",
            "turn_sell": "开启拐点卖出",
            "turn_pct": "拐点比例",
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(args.output, index=False, encoding="utf-8-sig")
    # 每个周期分别取 Top 10，便于横向比较，不与完整结果混在一起。
    total_summary = result_df.sort_values(
        ["周期(交易日)", "最后总盈亏(元)"], ascending=[True, False]
    ).copy()
    total_summary["周期内排名"] = total_summary.groupby("周期(交易日)").cumcount() + 1
    total_summary = total_summary[total_summary["周期内排名"] <= 10]

    t_summary = result_df.sort_values(
        ["周期(交易日)", "做T收益(元)"], ascending=[True, False]
    ).copy()
    t_summary["周期内排名"] = t_summary.groupby("周期(交易日)").cumcount() + 1
    t_summary = t_summary[t_summary["周期内排名"] <= 10]

    total_summary = total_summary[["周期(交易日)", "周期内排名"] + [c for c in result_df.columns if c != "周期(交易日)"]]
    t_summary = t_summary[["周期(交易日)", "周期内排名"] + [c for c in result_df.columns if c != "周期(交易日)"]]
    total_path = args.output.with_name(f"{args.output.stem}_总资产Top10.csv")
    t_path = args.output.with_name(f"{args.output.stem}_做TTop10.csv")
    total_summary.to_csv(total_path, index=False, encoding="utf-8-sig")
    t_summary.to_csv(t_path, index=False, encoding="utf-8-sig")
    print(f"完成 {len(result_df)}/{total} 组，结果已写入 {args.output}")
    print(f"各周期总资产 Top 10 已写入 {total_path}")
    print(f"各周期做T Top 10 已写入 {t_path}")
    print("\n总资产收益 Top 10：")
    print(result_df.head(10).to_string(index=False, float_format=lambda x: f"{x:.4f}"))
    print("\n做T收益 Top 10：")
    print(
        result_df.sort_values(["做T收益(元)", "总收益(元)"], ascending=False)
        .head(10)
        .to_string(index=False, float_format=lambda x: f"{x:.4f}")
    )


if __name__ == "__main__":
    main()
