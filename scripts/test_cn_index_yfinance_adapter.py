#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import pandas as pd


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from integrations.data_source import _fetch_stock_yfinance


TEST_SYMBOLS = [
    ("上证指数", "000001.SS"),
    ("深证成指", "399001.SZ"),
    ("创业板指", "399006.SZ"),
    ("沪深300", "000300.SS"),
]


def _summarize_frame(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "first_row": df.iloc[0].to_dict() if not df.empty else {},
        "last_row": df.iloc[-1].to_dict() if not df.empty else {},
    }


def main() -> int:
    print("=" * 60)
    print("CN Index yfinance Adapter Test")
    print("=" * 60)

    end_day = date.today()
    start_day = end_day - timedelta(days=45)
    start_s = start_day.strftime("%Y%m%d")
    end_s = end_day.strftime("%Y%m%d")

    print(f"Date range: {start_day} to {end_day}")
    print(f"Using adapter: integrations.data_source._fetch_stock_yfinance")

    failures = 0
    for name, symbol in TEST_SYMBOLS:
        print(f"\n[{name}] {symbol}")
        try:
            frame = _fetch_stock_yfinance(symbol, start_s, end_s)
            if frame is None or frame.empty:
                failures += 1
                print("  ❌ empty dataframe")
                continue
            summary = _summarize_frame(frame)
            print(f"  ✓ rows={summary['rows']}")
            print(f"  ✓ columns={summary['columns']}")
            print(f"  ✓ first_row={summary['first_row']}")
            print(f"  ✓ last_row={summary['last_row']}")
            required = ["日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率", "振幅"]
            missing = [col for col in required if col not in frame.columns]
            if missing:
                failures += 1
                print(f"  ❌ missing columns={missing}")
            else:
                print("  ✓ normalized schema complete")
        except Exception as exc:
            failures += 1
            print(f"  ❌ failed: {type(exc).__name__}: {exc}")

    print("\n" + "=" * 60)
    if failures:
        print(f"Finished with {failures} failure(s)")
        return 1
    print("All probes passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
