#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""低频监测新浪 1 分钟接口是否触发限频。

每轮只请求一次 stock_zh_a_minute；默认每 120 秒执行一轮。成功数据会写入
data/grid_minute_cache.sqlite3 的 minute_bars_1m 表，便于后续网格回测复用。
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

from grid_strategy_backtest import (
    DEFAULT_DB_PATH,
    _connect_minute_db,
    _normalise_minute_frame,
    _upsert_minute_bars,
    fetch_sina_minute,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="588060")
    parser.add_argument("--interval", type=int, default=120, help="请求间隔秒数，默认 120")
    parser.add_argument("--cycles", type=int, default=5, help="测试轮数，默认 5 轮")
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    if args.interval < 1 or args.cycles < 1:
        raise ValueError("--interval 和 --cycles 必须大于 0")

    conn = _connect_minute_db(args.db_path)
    try:
        for index in range(args.cycles):
            started = time.monotonic()
            stamp = datetime.now().isoformat(timespec="seconds")
            try:
                frame = _normalise_minute_frame(fetch_sina_minute(args.symbol, "1"))
                rows = _upsert_minute_bars(conn, args.symbol, "1", frame)
                dates = sorted(frame["trade_date"].dropna().unique())
                print(f"[{stamp}] cycle={index + 1}/{args.cycles} OK rows={len(frame)} upserted={rows} dates={dates[-3:]}", flush=True)
            except Exception as exc:
                print(f"[{stamp}] cycle={index + 1}/{args.cycles} FAIL {type(exc).__name__}: {exc}", flush=True)
            if index + 1 < args.cycles:
                elapsed = time.monotonic() - started
                time.sleep(max(args.interval - elapsed, 0))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
