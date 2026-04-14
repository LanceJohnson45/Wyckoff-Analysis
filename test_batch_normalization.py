#!/usr/bin/env python3
import os
import sys
from datetime import date, timedelta

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.us_sp500_maintenance import _download_batch, _normalize_batch_download
import pandas as pd


def test_batch_normalization():
    print("=" * 60)
    print("SP500 Batch Normalization Test")
    print("=" * 60)
    
    test_symbols = ["AAPL", "MSFT", "GOOGL"]
    end_day = date.today()
    start_day = end_day - timedelta(days=30)
    
    print(f"\n1. Downloading batch: {test_symbols}")
    print(f"   Date range: {start_day} to {end_day}")
    
    try:
        frames = _download_batch(test_symbols, start_day, end_day)
        print(f"   ✓ Batch download returned {len(frames)} dataframes")
    except Exception as e:
        print(f"   ❌ Batch download failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n2. Checking each symbol's normalized data:")
    
    for symbol in test_symbols:
        frame = frames.get(symbol)
        
        if frame is None:
            print(f"\n❌ {symbol}: Not in batch results")
            continue
        
        if frame.empty:
            print(f"\n❌ {symbol}: Empty DataFrame")
            continue
        
        print(f"\n✓ {symbol}:")
        print(f"   Rows: {len(frame)}")
        print(f"   Columns: {list(frame.columns)}")
        
        required_cols = ['日期', '开盘', '最高', '最低', '收盘', '成交量']
        missing = [c for c in required_cols if c not in frame.columns]
        if missing:
            print(f"   ⚠️  Missing columns: {missing}")
        
        print(f"   First row:")
        first_row = frame.iloc[0]
        for col in ['日期', '开盘', '最高', '最低', '收盘', '成交量']:
            if col in first_row:
                print(f"     {col}: {first_row[col]}")
        
        print(f"   Last row:")
        last_row = frame.iloc[-1]
        for col in ['日期', '开盘', '最高', '最低', '收盘', '成交量']:
            if col in last_row:
                print(f"     {col}: {last_row[col]}")
        
        date_col = frame['日期']
        print(f"   Date column type: {date_col.dtype}")
        print(f"   Date sample: {date_col.iloc[0]}")
        
        if pd.api.types.is_numeric_dtype(frame['收盘']):
            print(f"   ✓ 收盘 is numeric")
        else:
            print(f"   ⚠️  收盘 is not numeric: {frame['收盘'].dtype}")
    
    print("\n" + "=" * 60)
    print("Test completed")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(test_batch_normalization())
