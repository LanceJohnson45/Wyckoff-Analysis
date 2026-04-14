#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Local test script for US stock data upload to Supabase.

Usage:
1. Copy .env.example to .env
2. Fill in SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
3. Run: python test_local_us_upload.py
"""

import os
import sys
import time
from datetime import date, timedelta

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed, using system environment variables only")

import pandas as pd
from core.stock_cache import normalize_hist_df, upsert_cache_data, get_cache_meta


def generate_mock_us_data(symbol: str, days: int = 30) -> pd.DataFrame:
    """Generate mock US stock data in the expected format."""
    end_date = date.today()
    dates = pd.date_range(end=end_date, periods=days, freq='D')
    
    base_price = 100.0
    data = {
        '日期': [d.strftime('%Y-%m-%d') for d in dates],
        '开盘': [base_price + i * 0.5 for i in range(days)],
        '最高': [base_price + i * 0.5 + 2.0 for i in range(days)],
        '最低': [base_price + i * 0.5 - 1.0 for i in range(days)],
        '收盘': [base_price + i * 0.5 + 0.5 for i in range(days)],
        '成交量': [1000000 + i * 10000 for i in range(days)],
    }
    
    df = pd.DataFrame(data)
    
    df['成交额'] = df['收盘'] * df['成交量']
    df['涨跌幅'] = df['收盘'].pct_change() * 100.0
    if not df.empty:
        df.loc[df.index[0], '涨跌幅'] = 0.0
    df['换手率'] = pd.NA
    prev_close = df['收盘'].shift(1)
    if not df.empty:
        prev_close.iloc[0] = df.loc[df.index[0], '开盘']
    df['振幅'] = ((df['最高'] - df['最低']) / prev_close * 100.0)
    
    return df


def test_upload_with_prefix(symbol: str, use_prefix: bool = True) -> dict:
    """Test uploading mock data with or without US: prefix."""
    result = {
        "symbol": symbol,
        "use_prefix": use_prefix,
        "generate_ok": False,
        "normalize_ok": False,
        "upsert_ok": False,
        "verify_ok": False,
        "error": None,
    }
    
    try:
        print(f"\n{'='*60}")
        print(f"Testing {symbol} (prefix={use_prefix})")
        print(f"{'='*60}")
        
        print(f"\n[1] Generating mock data...")
        df = generate_mock_us_data(symbol, days=30)
        result["generate_ok"] = True
        print(f"    ✓ Generated {len(df)} rows")
        print(f"    Columns: {list(df.columns)}")
        print(f"    First date: {df['日期'].iloc[0]}")
        print(f"    Last date: {df['日期'].iloc[-1]}")
        
        print(f"\n[2] Normalizing data...")
        norm = normalize_hist_df(df)
        if norm.empty:
            result["error"] = "normalize returned empty"
            print(f"    ❌ Normalize failed")
            return result
        
        result["normalize_ok"] = True
        latest_date = pd.to_datetime(norm["date"], errors="coerce").dropna().max()
        print(f"    ✓ Normalized to {len(norm)} rows")
        print(f"    Latest date: {latest_date.date()}")
        print(f"    Columns: {list(norm.columns)}")
        
        storage_symbol = f"US:{symbol}" if use_prefix else symbol
        print(f"\n[3] Upserting to Supabase (symbol='{storage_symbol}')...")
        
        print(f"    Environment check:")
        print(f"      SUPABASE_URL: {'SET' if os.getenv('SUPABASE_URL') else 'NOT SET'}")
        print(f"      SUPABASE_SERVICE_ROLE_KEY: {'SET' if os.getenv('SUPABASE_SERVICE_ROLE_KEY') else 'NOT SET'}")
        
        ok = upsert_cache_data(
            symbol=storage_symbol,
            adjust="qfq",
            source="test_local",
            df=norm,
            context="background",
        )
        
        if not ok:
            result["error"] = "upsert returned False"
            print(f"    ❌ Upsert failed")
            return result
        
        result["upsert_ok"] = True
        print(f"    ✓ Upsert succeeded")
        
        print(f"\n[4] Verifying write...")
        
        for attempt, delay in enumerate([0.0, 0.5, 1.0, 2.0], start=1):
            if delay > 0:
                print(f"    Attempt {attempt}: waiting {delay}s...")
                time.sleep(delay)
            else:
                print(f"    Attempt {attempt}: immediate check...")
            
            meta = get_cache_meta(storage_symbol, "qfq", context="background")
            
            if meta is None:
                print(f"      ⚠️  Meta is None")
                continue
            
            print(f"      Meta: start={meta.start_date}, end={meta.end_date}")
            print(f"      Expected end: {latest_date.date()}")
            
            if meta.end_date >= latest_date.date():
                result["verify_ok"] = True
                print(f"      ✓ Verify passed!")
                break
            else:
                print(f"      ⚠️  End date mismatch: {meta.end_date} < {latest_date.date()}")
        
        if not result["verify_ok"]:
            result["error"] = f"verify failed: meta.end_date={meta.end_date if meta else None} < {latest_date.date()}"
            print(f"    ❌ Verify failed after all attempts")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"    ❌ Exception: {e}")
        import traceback
        traceback.print_exc()
    
    return result


def main():
    print("\n" + "="*60)
    print("Local US Stock Upload Test")
    print("="*60)
    
    print("\nThis script will:")
    print("1. Generate mock US stock data")
    print("2. Test upload WITH 'US:' prefix")
    print("3. Test upload WITHOUT 'US:' prefix")
    print("4. Compare results")
    
    print("\nEnvironment check:")
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    if not url or not key:
        print("\n❌ ERROR: Missing environment variables!")
        print("\nPlease set in .env file:")
        print("  SUPABASE_URL=your_supabase_url")
        print("  SUPABASE_SERVICE_ROLE_KEY=your_service_role_key")
        return 1
    
    print(f"  SUPABASE_URL: {url[:30]}...")
    print(f"  SUPABASE_SERVICE_ROLE_KEY: (length={len(key)})")
    
    results = []
    
    print("\n" + "="*60)
    print("Test 1: WITH 'US:' prefix (current implementation)")
    print("="*60)
    result1 = test_upload_with_prefix("TEST_AAPL", use_prefix=True)
    results.append(result1)
    
    print("\n" + "="*60)
    print("Test 2: WITHOUT 'US:' prefix (like A-share)")
    print("="*60)
    result2 = test_upload_with_prefix("TEST_MSFT", use_prefix=False)
    results.append(result2)
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    for r in results:
        prefix_str = "WITH prefix" if r["use_prefix"] else "WITHOUT prefix"
        status = "✓" if r["verify_ok"] else "❌"
        print(f"\n{status} {r['symbol']} ({prefix_str}):")
        print(f"   Generate: {r['generate_ok']}")
        print(f"   Normalize: {r['normalize_ok']}")
        print(f"   Upsert: {r['upsert_ok']}")
        print(f"   Verify: {r['verify_ok']}")
        if r["error"]:
            print(f"   Error: {r['error']}")
    
    success_count = sum(1 for r in results if r["verify_ok"])
    print(f"\nSuccess rate: {success_count}/{len(results)}")
    
    if success_count == 0:
        print("\n⚠️  Both tests failed. This suggests a fundamental issue with:")
        print("   - Supabase client configuration")
        print("   - Database permissions")
        print("   - Network connectivity")
    elif success_count == 1:
        print("\n⚠️  One test passed, one failed. This suggests:")
        if results[0]["verify_ok"] and not results[1]["verify_ok"]:
            print("   - The 'US:' prefix might be causing issues")
        else:
            print("   - The issue might be intermittent or timing-related")
    else:
        print("\n✓ Both tests passed! The issue might be specific to GitHub Actions environment.")
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
