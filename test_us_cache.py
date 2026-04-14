#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test US stock cache write/read cycle to diagnose verification failures."""

import os
import sys
import time
from datetime import date, timedelta

import pandas as pd

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.stock_cache import get_cache_meta, normalize_hist_df, upsert_cache_data
from integrations.data_source import _fetch_stock_yfinance


def test_single_symbol(symbol: str, days: int = 30) -> dict:
    """Test write/verify cycle for a single US symbol."""
    result = {
        "symbol": symbol,
        "fetch_ok": False,
        "rows_fetched": 0,
        "upsert_ok": False,
        "verify_ok": False,
        "meta_before": None,
        "meta_after": None,
        "error": None,
    }

    try:
        # 1. Fetch data
        end_day = date.today()
        start_day = end_day - timedelta(days=days)
        print(f"\n[{symbol}] Fetching {start_day} to {end_day}...")
        
        df = _fetch_stock_yfinance(
            symbol, 
            start_day.strftime("%Y%m%d"), 
            end_day.strftime("%Y%m%d")
        )
        
        if df is None or df.empty:
            result["error"] = "fetch returned empty"
            print(f"[{symbol}] ❌ Fetch failed: empty data")
            return result
        
        result["fetch_ok"] = True
        result["rows_fetched"] = len(df)
        print(f"[{symbol}] ✓ Fetched {len(df)} rows")

        # 2. Check meta before
        meta_before = get_cache_meta(f"US:{symbol}", "qfq", context="background")
        result["meta_before"] = {
            "exists": meta_before is not None,
            "end_date": str(meta_before.end_date) if meta_before else None,
        }
        print(f"[{symbol}] Meta before: {result['meta_before']}")

        # 3. Normalize
        norm = normalize_hist_df(df)
        if norm.empty:
            result["error"] = "normalize returned empty"
            print(f"[{symbol}] ❌ Normalize failed")
            return result
        
        latest_date = pd.to_datetime(norm["date"], errors="coerce").dropna().max()
        if pd.isna(latest_date):
            result["error"] = "no valid dates after normalize"
            print(f"[{symbol}] ❌ No valid dates")
            return result
        
        print(f"[{symbol}] Latest date in data: {latest_date.date()}")

        # 4. Upsert
        print(f"[{symbol}] Upserting...")
        ok = upsert_cache_data(
            symbol=f"US:{symbol}",
            adjust="qfq",
            source="test_script",
            df=norm,
            context="background",
        )
        result["upsert_ok"] = ok
        
        if not ok:
            result["error"] = "upsert returned False"
            print(f"[{symbol}] ❌ Upsert failed")
            return result
        
        print(f"[{symbol}] ✓ Upsert succeeded")

        # 5. Wait and verify
        for delay in [0.1, 0.5, 1.0, 2.0]:
            print(f"[{symbol}] Waiting {delay}s before verify...")
            time.sleep(delay)
            
            meta_after = get_cache_meta(f"US:{symbol}", "qfq", context="background")
            result["meta_after"] = {
                "exists": meta_after is not None,
                "end_date": str(meta_after.end_date) if meta_after else None,
            }
            
            if meta_after is None:
                print(f"[{symbol}] ⚠️  Meta still None after {delay}s")
                continue
            
            if meta_after.end_date < latest_date.date():
                print(f"[{symbol}] ⚠️  Meta end_date {meta_after.end_date} < expected {latest_date.date()}")
                continue
            
            result["verify_ok"] = True
            print(f"[{symbol}] ✓ Verify passed after {delay}s delay")
            break
        
        if not result["verify_ok"]:
            result["error"] = f"verify failed: meta={result['meta_after']}"
            print(f"[{symbol}] ❌ Verify failed after all retries")

    except Exception as e:
        result["error"] = str(e)
        print(f"[{symbol}] ❌ Exception: {e}")
    
    return result


def main():
    print("=" * 60)
    print("US Stock Cache Write/Verify Test")
    print("=" * 60)
    
    # Test with 3 symbols
    test_symbols = ["AAPL", "MSFT", "GOOGL"]
    results = []
    
    for symbol in test_symbols:
        result = test_single_symbol(symbol, days=30)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for r in results:
        status = "✓" if r["verify_ok"] else "❌"
        print(f"{status} {r['symbol']}: fetch={r['fetch_ok']}, upsert={r['upsert_ok']}, verify={r['verify_ok']}")
        if r["error"]:
            print(f"   Error: {r['error']}")
    
    success_count = sum(1 for r in results if r["verify_ok"])
    print(f"\nSuccess rate: {success_count}/{len(results)}")
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
