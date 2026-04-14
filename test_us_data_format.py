#!/usr/bin/env python3
import os
import sys
from datetime import date, timedelta

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import pandas as pd
from scripts.us_sp500_maintenance import _download_batch
from core.stock_cache import normalize_hist_df
import json


def main():
    print("=" * 60)
    print("US Data Format Inspection")
    print("=" * 60)
    
    symbol = "AAPL"
    end_day = date.today()
    start_day = end_day - timedelta(days=30)
    
    print(f"\n1. Downloading {symbol}...")
    frames = _download_batch([symbol], start_day, end_day)
    df = frames.get(symbol)
    
    if df is None or df.empty:
        print("❌ Download failed")
        return 1
    
    print(f"✓ Downloaded {len(df)} rows")
    print(f"\n2. After _download_batch (Chinese columns):")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Dtypes:\n{df.dtypes}")
    print(f"\n   First row:")
    first_row = df.iloc[0].to_dict()
    for k, v in first_row.items():
        print(f"     {k}: {v} (type: {type(v).__name__})")
    
    print(f"\n3. After normalize_hist_df (English columns):")
    norm = normalize_hist_df(df)
    print(f"   Columns: {list(norm.columns)}")
    print(f"   Dtypes:\n{norm.dtypes}")
    print(f"\n   First row:")
    first_norm = norm.iloc[0].to_dict()
    for k, v in first_norm.items():
        print(f"     {k}: {v} (type: {type(v).__name__})")
    
    print(f"\n4. Payload that would be sent to Supabase:")
    payload = norm.copy()
    payload["date"] = payload["date"].astype(str)
    payload["symbol"] = f"US:{symbol}"
    payload["adjust"] = "qfq"
    payload["updated_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    
    record = payload.iloc[0].to_dict()
    print(f"\n   Record (as dict):")
    print(json.dumps(record, indent=2, default=str))
    
    print(f"\n5. Checking for problematic values:")
    for col in norm.columns:
        null_count = norm[col].isna().sum()
        if null_count > 0:
            print(f"   ⚠️  {col}: {null_count} null values")
        
        if col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg', 'amplitude']:
            inf_count = (norm[col] == float('inf')).sum() + (norm[col] == float('-inf')).sum()
            if inf_count > 0:
                print(f"   ⚠️  {col}: {inf_count} inf values")
    
    print(f"\n" + "=" * 60)
    print("Inspection complete")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
