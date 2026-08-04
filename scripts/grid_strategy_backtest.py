#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""四个 AkShare 分时接口探测 + 简单网格交易回测。

接口探测（每个接口只请求一次）：
  stock_zh_a_minute      新浪 1/5/15/30/60 分钟 K 线
  stock_zh_a_hist_min_em 东方财富 1/5/15/30/60 分钟 K 线
  stock_intraday_em      东方财富逐笔成交
  stock_intraday_sina    新浪大单逐笔成交

回测规则：以第一根 K 线收盘价为网格中枢；价格相对上次成交价上涨
up_pct 时卖出 sell_shares，下降 down_pct 时买入 buy_shares。若一根 K
线跨越多个网格，则逐级成交。买入受现金限制，卖出受持仓限制。每笔交易
成本均为成交金额 * fee_rate。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import deque
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable

import pandas as pd


DEFAULT_FEE_RATE = 0.0001
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "grid_minute_cache.sqlite3"


def _market_symbol(symbol: str) -> tuple[str, str]:
    code = str(symbol).strip().lower().replace("sh", "").replace("sz", "")
    if not code.isdigit() or len(code) != 6:
        raise ValueError(f"股票代码必须是 6 位数字，收到: {symbol!r}")
    prefix = "sh" if code.startswith(("5", "6", "9")) else "sz"
    return code, prefix + code


def fetch_sina_minute(symbol: str, period: str = "5") -> pd.DataFrame:
    import akshare as ak

    _, sina_symbol = _market_symbol(symbol)
    return ak.stock_zh_a_minute(symbol=sina_symbol, period=period, adjust="")


def fetch_eastmoney_minute(
    symbol: str,
    period: str = "5",
    start_date: str = "1979-09-01 09:32:00",
    end_date: str = "2222-01-01 09:32:00",
) -> pd.DataFrame:
    import akshare as ak

    code, _ = _market_symbol(symbol)
    return ak.stock_zh_a_hist_min_em(
        symbol=code,
        start_date=start_date,
        end_date=end_date,
        period=period,
        adjust="",
    )


def fetch_eastmoney_intraday(symbol: str) -> pd.DataFrame:
    import akshare as ak

    code, _ = _market_symbol(symbol)
    return ak.stock_intraday_em(symbol=code)


def fetch_sina_intraday(symbol: str, trade_date: str) -> pd.DataFrame:
    import akshare as ak

    _, sina_symbol = _market_symbol(symbol)
    return ak.stock_intraday_sina(symbol=sina_symbol, date=trade_date)


@dataclass
class GridResult:
    initial_cash: float
    initial_shares: int
    initial_price: float
    final_cash: float
    final_shares: int
    last_price: float
    buy_count: int
    sell_count: int
    fees: float
    t_profit: float
    t_pairs: int

    @property
    def final_asset(self) -> float:
        return self.final_cash + self.final_shares * self.last_price

    @property
    def initial_asset(self) -> float:
        return self.initial_cash + self.initial_shares * self.initial_price

    @property
    def profit(self) -> float:
        return self.final_asset - self.initial_asset

    @property
    def return_pct(self) -> float:
        return self.profit / self.initial_asset * 100 if self.initial_asset else 0.0


def _normalise_close_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """兼容新浪/东财列名，返回按时间排序的 date、close。"""
    time_col = next((c for c in ("day", "时间") if c in frame.columns), None)
    close_col = next((c for c in ("close", "收盘") if c in frame.columns), None)
    if time_col is None or close_col is None:
        raise ValueError(f"无法识别行情列，实际列: {list(frame.columns)}")
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(frame[time_col], errors="coerce"),
            "close": pd.to_numeric(frame[close_col], errors="coerce"),
        }
    ).dropna()
    return out.sort_values("date").drop_duplicates("date").reset_index(drop=True)


def _normalise_minute_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """统一新浪 5 分钟 K 线字段，便于写入本地 SQLite。"""
    if frame is None or frame.empty:
        return pd.DataFrame(columns=["bar_time", "trade_date", "open", "high", "low", "close", "volume", "amount"])
    time_col = "day" if "day" in frame.columns else "时间" if "时间" in frame.columns else None
    if time_col is None:
        raise ValueError(f"无法识别分钟行情时间列: {list(frame.columns)}")
    rename = {"开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"}
    out = frame.rename(columns=rename).copy()
    out["bar_time"] = pd.to_datetime(out[time_col], errors="coerce")
    out["trade_date"] = out["bar_time"].dt.strftime("%Y-%m-%d")
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out[["bar_time", "trade_date", "open", "high", "low", "close", "volume", "amount"]]
    return out.dropna(subset=["bar_time", "close"]).drop_duplicates("bar_time").sort_values("bar_time").reset_index(drop=True)


def _connect_minute_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    schema = """(
            symbol TEXT NOT NULL,
            period TEXT NOT NULL,
            bar_time TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, amount REAL,
            source TEXT NOT NULL DEFAULT 'sina',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, period, bar_time)
        )"""
    # 1 分钟单独建表，避免与 5 分钟数据混存；保留通用 period 字段便于查询。
    for table in ("minute_bars_1m", "minute_bars_5m"):
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} {schema}")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_symbol_date ON {table}(symbol, trade_date)")
    conn.commit()
    return conn


def _minute_table(period: str) -> str:
    if period == "1":
        return "minute_bars_1m"
    if period == "5":
        return "minute_bars_5m"
    raise ValueError("本地分钟缓存目前只支持 1 分钟或 5 分钟")


def _load_cached_minute(conn: sqlite3.Connection, symbol: str, period: str, dates: set[str]) -> pd.DataFrame:
    if not dates:
        return pd.DataFrame()
    marks = ",".join("?" for _ in dates)
    table = _minute_table(period)
    sql = f"SELECT bar_time, trade_date, open, high, low, close, volume, amount FROM {table} WHERE symbol=? AND period=? AND trade_date IN ({marks}) ORDER BY bar_time"
    return pd.read_sql_query(sql, conn, params=[symbol, period, *sorted(dates)], parse_dates=["bar_time"])


def _upsert_minute_bars(conn: sqlite3.Connection, symbol: str, period: str, frame: pd.DataFrame) -> int:
    rows = []
    for row in frame.itertuples(index=False):
        rows.append((symbol, period, row.bar_time.isoformat(), row.trade_date, row.open, row.high, row.low, row.close, row.volume, row.amount))
    table = _minute_table(period)
    conn.executemany(
        f"""INSERT INTO {table}(symbol, period, bar_time, trade_date, open, high, low, close, volume, amount)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(symbol, period, bar_time) DO UPDATE SET
             trade_date=excluded.trade_date, open=excluded.open, high=excluded.high,
             low=excluded.low, close=excluded.close, volume=excluded.volume,
             amount=excluded.amount, updated_at=CURRENT_TIMESTAMP""",
        rows,
    )
    conn.commit()
    return len(rows)


def _recent_trade_dates(days: int) -> list[str]:
    """优先使用仓库已有交易日缓存，不为日期判断额外请求接口。"""
    calendar_path = Path(__file__).resolve().parent.parent / "data" / "trade_dates_cache.json"
    try:
        raw = json.loads(calendar_path.read_text(encoding="utf-8"))
        dates = sorted({str(x)[:10] for x in raw if str(x)[:10] <= date.today().isoformat()})
        if dates:
            return dates[-days:]
    except Exception:
        pass
    return [d.strftime("%Y-%m-%d") for d in pd.bdate_range(end=date.today(), periods=days)]


def load_or_fill_sina_cache(symbol: str, period: str, days: int, db_path: Path, *, refresh: bool = False) -> tuple[pd.DataFrame, int, list[str]]:
    """本地优先；缺少目标交易日时新浪只请求一次，然后写入 SQLite。"""
    target_dates = _recent_trade_dates(days)
    target_set = set(target_dates)
    conn = _connect_minute_db(db_path)
    try:
        cached = _load_cached_minute(conn, symbol, period, target_set)
        cached_days = set(cached["trade_date"].astype(str)) if not cached.empty else set()
        missing = target_set - cached_days
        fetched_rows = 0
        # 新浪接口按固定条数返回滚动窗口；1 分钟数据在交易日内可能只覆盖
        # 9 个交易日。已有最新交易日且只缺最老一天时，不重复请求，避免每次
        # 回测都打新浪接口。
        latest_ok = bool(cached_days) and max(cached_days) >= target_dates[-1]
        window_sufficient = len(cached_days) >= max(days - 1, 1)
        should_fetch = refresh or not cached_days or (missing and not (latest_ok and window_sufficient))
        if should_fetch:
            print(f"[cache] 本地已有 {len(cached_days)}/{len(target_set)} 个交易日，新浪仅请求一次补齐窗口")
            fetched = _normalise_minute_frame(fetch_sina_minute(symbol, period))
            fetched_rows = _upsert_minute_bars(conn, symbol, period, fetched)
            cached = _load_cached_minute(conn, symbol, period, target_set)
        final_days = sorted(set(cached["trade_date"].astype(str))) if not cached.empty else []
        return cached, fetched_rows, final_days
    finally:
        conn.close()


def backtest_grid(
    prices: pd.Series,
    *,
    up_pct: float,
    down_pct: float,
    sell_shares: int,
    buy_shares: int,
    initial_cash: float,
    initial_shares: int,
    timestamps: pd.Series | None = None,
    fee_rate: float = DEFAULT_FEE_RATE,
    enable_turn_buy: bool = False,
    enable_turn_sell: bool = False,
    turn_pct: float = 0.005,
) -> GridResult:
    if prices.empty:
        raise ValueError("没有可回测的价格数据")
    if up_pct <= 0 or down_pct <= 0:
        raise ValueError("上涨和下跌网格比例必须大于 0")
    if sell_shares <= 0 or buy_shares <= 0:
        raise ValueError("买卖股数必须大于 0")
    if turn_pct <= 0:
        raise ValueError("拐点比例必须大于 0")
    if initial_cash < 0 or initial_shares < 0:
        raise ValueError("初始现金和初始持仓不能为负数")

    cash = float(initial_cash)
    shares = int(initial_shares)
    if timestamps is None:
        # 兼容只传价格序列的单元测试；实际行情回测会传入 bar_time。
        day_keys = pd.Series(range(len(prices)), index=prices.index)
    else:
        day_keys = pd.to_datetime(timestamps, errors="coerce").dt.date
        if len(day_keys) != len(prices):
            raise ValueError("timestamps 与 prices 长度不一致")
    current_day = None
    sellable_shares = shares
    anchor = float(prices.iloc[0])
    first_price = anchor
    fees = 0.0
    buy_count = sell_count = 0
    t_profit = 0.0
    t_pairs = 0
    # 记录尚未买回的卖出仓位：(剩余股数，扣除卖出成本后的每股收入)。
    open_sells: deque[list[float]] = deque()
    pending_buy_low: float | None = None
    pending_sell_high: float | None = None

    for position, raw_price in enumerate(prices):
        trade_day = day_keys.iloc[position]
        if trade_day != current_day:
            current_day = trade_day
            # T+1：新交易日开始时，前一交易日买入的股票才可卖出。
            sellable_shares = shares
        price = float(raw_price)
        if price <= 0:
            continue
        if enable_turn_buy:
            if pending_buy_low is None:
                if price <= anchor * (1 - down_pct):
                    pending_buy_low = price
            else:
                pending_buy_low = min(pending_buy_low, price)
                if price >= pending_buy_low * (1 + turn_pct):
                    amount = price * buy_shares
                    total = amount * (1 + fee_rate)
                    if cash >= total:
                        shares += buy_shares
                        cash -= total
                        fees += amount * fee_rate
                        buy_count += 1
                        remaining = float(buy_shares)
                        buy_cost_per_share = price * (1 + fee_rate)
                        while remaining > 0 and open_sells:
                            sold_qty, sell_net_per_share = open_sells[0]
                            matched = min(remaining, sold_qty)
                            t_profit += matched * (sell_net_per_share - buy_cost_per_share)
                            t_pairs += 1
                            remaining -= matched
                            sold_qty -= matched
                            if sold_qty <= 0:
                                open_sells.popleft()
                            else:
                                open_sells[0][0] = sold_qty
                        anchor *= 1 - down_pct
                    pending_buy_low = None

        if enable_turn_sell:
            if pending_sell_high is None:
                if price >= anchor * (1 + up_pct):
                    pending_sell_high = price
            else:
                pending_sell_high = max(pending_sell_high, price)
                if price <= pending_sell_high * (1 - turn_pct):
                    if sellable_shares >= sell_shares:
                        amount = price * sell_shares
                        fee = amount * fee_rate
                        shares -= sell_shares
                        sellable_shares -= sell_shares
                        cash += amount - fee
                        fees += fee
                        sell_count += 1
                        open_sells.append([float(sell_shares), price * (1 - fee_rate)])
                        anchor *= 1 + up_pct
                    pending_sell_high = None

        # 未开启拐点确认的一侧，仍按原来的即时网格成交。
        if not enable_turn_sell:
            while price >= anchor * (1 + up_pct):
                if sellable_shares < sell_shares:
                    break
                amount = price * sell_shares
                fee = amount * fee_rate
                shares -= sell_shares
                sellable_shares -= sell_shares
                cash += amount - fee
                fees += fee
                sell_count += 1
                open_sells.append([float(sell_shares), price * (1 - fee_rate)])
                anchor *= 1 + up_pct
        if not enable_turn_buy:
            while price <= anchor * (1 - down_pct):
                amount = price * buy_shares
                total = amount * (1 + fee_rate)
                if cash < total:
                    break
                shares += buy_shares
                cash -= total
                fees += amount * fee_rate
                buy_count += 1
                # 先卖后买回的部分视为做 T；买入超过历史卖出量的部分只是加仓。
                remaining = float(buy_shares)
                buy_cost_per_share = price * (1 + fee_rate)
                while remaining > 0 and open_sells:
                    sold_qty, sell_net_per_share = open_sells[0]
                    matched = min(remaining, sold_qty)
                    t_profit += matched * (sell_net_per_share - buy_cost_per_share)
                    t_pairs += 1
                    remaining -= matched
                    sold_qty -= matched
                    if sold_qty <= 0:
                        open_sells.popleft()
                    else:
                        open_sells[0][0] = sold_qty
                anchor *= 1 - down_pct

    return GridResult(
        initial_cash=initial_cash,
        initial_shares=initial_shares,
        initial_price=first_price,
        final_cash=cash,
        final_shares=shares,
        last_price=float(prices.iloc[-1]),
        buy_count=buy_count,
        sell_count=sell_count,
        fees=fees,
        t_profit=t_profit,
        t_pairs=t_pairs,
    )


def probe(args: argparse.Namespace) -> None:
    calls: list[tuple[str, Callable[[], pd.DataFrame]]] = [
        ("sina_minute", lambda: fetch_sina_minute(args.symbol, args.period)),
        (
            "eastmoney_minute",
            lambda: fetch_eastmoney_minute(args.symbol, args.period),
        ),
        ("eastmoney_intraday", lambda: fetch_eastmoney_intraday(args.symbol)),
        (
            "sina_intraday",
            lambda: fetch_sina_intraday(args.symbol, args.trade_date),
        ),
    ]
    for name, call in calls:
        try:
            frame = call()  # deliberately exactly one request per interface
            print(f"[{name}] OK rows={len(frame)} columns={list(frame.columns)}")
            print(frame.head(3).to_string(index=False))
        except Exception as exc:
            print(f"[{name}] FAIL {type(exc).__name__}: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="588060", help="6 位股票代码")
    parser.add_argument("--period", choices=("1", "5", "15", "30", "60"), default="1", help="分钟周期，默认 1 分钟")
    parser.add_argument("--trade-date", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--probe", action="store_true", help="四个接口各请求一次")
    parser.add_argument("--source", choices=("sina", "eastmoney"), default="sina", help="回测使用的 5 分钟 K 线接口")
    parser.add_argument("-d", "--days", type=int, default=10, help="回测过去多少个交易日")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH, help="分钟行情 SQLite 缓存路径")
    parser.add_argument("--refresh", action="store_true", help="忽略本地覆盖情况，新浪请求一次刷新缓存")
    parser.add_argument("--up-pct", type=float, default=0.02, help="上涨网格比例，例如 0.02")
    parser.add_argument("--down-pct", type=float, default=0.02, help="下跌网格比例，例如 0.02")
    parser.add_argument("--sell-shares", type=int, default=100, metavar="M")
    parser.add_argument("--buy-shares", type=int, default=100, metavar="K")
    parser.add_argument("--initial-cash", type=float, default=300000.0)
    parser.add_argument("--initial-shares", type=int, default=20000)
    parser.add_argument("--fee-rate", type=float, default=DEFAULT_FEE_RATE)
    parser.add_argument("--enable-turn-buy", action="store_true", help="开启拐点确认买入")
    parser.add_argument("--enable-turn-sell", action="store_true", help="开启拐点确认卖出")
    parser.add_argument("--turn-pct", type=float, default=0.005, choices=(0.005, 0.01), help="拐点比例：0.005=0.5%%，0.01=1%%")
    args = parser.parse_args()

    if args.probe:
        probe(args)
        return

    if args.days <= 0:
        raise ValueError("--days 必须大于 0")
    if args.source == "sina":
        frame, fetched_rows, cached_days = load_or_fill_sina_cache(
            args.symbol, args.period, args.days, args.db_path, refresh=args.refresh
        )
        if not cached_days:
            raise RuntimeError("新浪接口没有返回目标交易日的分钟数据")
        print(f"[cache] SQLite={args.db_path}，本次写入 {fetched_rows} 条，实际覆盖 {len(cached_days)} 个交易日")
    else:
        frame = fetch_eastmoney_minute(args.symbol, args.period)
        fetched_rows = len(frame)
        cached_days = []
    frame = frame.copy()
    if "bar_time" in frame.columns:
        frame["date"] = frame["bar_time"]
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.dropna(subset=["date", "close"])
        frame = frame.sort_values("date").reset_index(drop=True)
        prices = frame["close"]
    else:
        prices = _normalise_close_frame(frame)["close"]
    result = backtest_grid(
        prices,
        up_pct=args.up_pct,
        down_pct=args.down_pct,
        sell_shares=args.sell_shares,
        buy_shares=args.buy_shares,
        initial_cash=args.initial_cash,
        initial_shares=args.initial_shares,
        timestamps=frame["date"],
        fee_rate=args.fee_rate,
        enable_turn_buy=args.enable_turn_buy,
        enable_turn_sell=args.enable_turn_sell,
        turn_pct=args.turn_pct,
    )
    print(f"数据: {len(prices)} 根 {args.period} 分钟 K 线，{prices.iloc[0]:.3f} -> {prices.iloc[-1]:.3f}")
    print(f"网格: 上涨 {args.up_pct:.2%} 卖 {args.sell_shares} 股；下跌 {args.down_pct:.2%} 买 {args.buy_shares} 股")
    print(f"交易次数: 买 {result.buy_count}，卖 {result.sell_count}；交易成本: {result.fees:.2f} 元")
    print(f"做T收益: {result.t_profit:.2f} 元（配对 {result.t_pairs} 笔，已扣交易成本）")
    print(f"期末现金: {result.final_cash:.2f} 元；期末持仓: {result.final_shares} 股")
    print(f"期末总资产: {result.final_asset:.2f} 元；收益: {result.profit:.2f} 元（{result.return_pct:.2f}%）")


if __name__ == "__main__":
    main()
