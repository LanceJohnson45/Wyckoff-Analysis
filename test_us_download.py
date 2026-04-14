#!/usr/bin/env python3
import os
import sys
from datetime import date, timedelta

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.us_sp500_maintenance import _download_batch, _normalize_batch_download
import pandas as pd


def main():
    print("=" * 60)
    print("US Stock Download Test")
    print("=" * 60)
    
    test_symbols = ["AAPL", "MSFT", "GOOGL"]
    end_day = date.today()
    start_day = end_day - timedelta(days=30)
    
    print(f"\nDownloading {len(test_symbols)} symbols: {test_symbols}")
    print(f"Date range: {start_day} to {end_day}")
    
    try:
        frames = _download_batch(test_symbols, start_day, end_day)
        print(f"\n✓ Download completed")
        print(f"Returned {len(frames)} dataframes")
        
        for symbol in test_symbols:
            frame = frames.get(symbol)
            if frame is None:
                print(f"\n❌ {symbol}: No data returned")
                continue
            
            if frame.empty:
                print(f"\n❌ {symbol}: Empty dataframe")
                continue
            
            print(f"\n✓ {symbol}:")
            print(f"   Rows: {len(frame)}")
            print(f"   Columns: {list(frame.columns)}")
            print(f"   First row:")
            print(f"   {frame.iloc[0].to_dict()}")
            print(f"   Last row:")
            print(f"   {frame.iloc[-1].to_dict()}")
            
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
