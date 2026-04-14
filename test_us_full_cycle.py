#!/usr/bin/env python3
import os
import sys
import time
from datetime import date, timedelta

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scripts.us_sp500_maintenance import _download_batch
from core.stock_cache import normalize_hist_df, upsert_cache_data, get_cache_meta
import pandas as pd


def test_full_cycle(symbol: str) -> dict:
    result = {
        "symbol": symbol,
        "download_ok": False,
        "normalize_ok": False,
        "upsert_ok": False,
        "verify_ok": False,
        "error": None,
    }
    
    try:
        end_day = date.today()
        start_day = end_day - timedelta(days=30)
        
        print(f"\n[{symbol}] Step 1: Download")
        frames = _download_batch([symbol], start_day, end_day)
        df = frames.get(symbol)
        
        if df is None or df.empty:
            result["error"] = "download empty"
            print(f"[{symbol}] ❌ Download failed")
            return result
        
        result["download_ok"] = True
        print(f"[{symbol}] ✓ Downloaded {len(df)} rows")
        
        print(f"[{symbol}] Step 2: Normalize")
        norm = normalize_hist_df(df)
        
        if norm.empty:
            result["error"] = "normalize empty"
            print(f"[{symbol}] ❌ Normalize failed")
            return result
        
        result["normalize_ok"] = True
        print(f"[{symbol}] ✓ Normalized to {len(norm)} rows")
        print(f"[{symbol}]   Columns: {list(norm.columns)}")
        print(f"[{symbol}]   First date: {norm['date'].iloc[0]}")
        print(f"[{symbol}]   Last date: {norm['date'].iloc[-1]}")
        
        latest_date = pd.to_datetime(norm["date"], errors="coerce").dropna().max()
        print(f"[{symbol}]   Latest date: {latest_date.date()}")
        
        print(f"[{symbol}] Step 3: Upsert")
        ok = upsert_cache_data(
            symbol=f"US:{symbol}",
            adjust="qfq",
            source="test",
            df=norm,
            context="background",
        )
        
        if not ok:
            result["error"] = "upsert returned False"
            print(f"[{symbol}] ❌ Upsert failed")
            return result
        
        result["upsert_ok"] = True
        print(f"[{symbol}] ✓ Upsert succeeded")
        
        print(f"[{symbol}] Step 4: Verify (with delays)")
        for delay in [0.1, 0.5, 1.0, 2.0]:
            time.sleep(delay)
            meta = get_cache_meta(f"US:{symbol}", "qfq", context="background")
            
            if meta is None:
                print(f"[{symbol}]   After {delay}s: meta is None")
                continue
            
            print(f"[{symbol}]   After {delay}s: meta.end_date={meta.end_date}")
            
            if meta.end_date >= latest_date.date():
                result["verify_ok"] = True
                print(f"[{symbol}] ✓ Verify passed after {delay}s")
                break
        
        if not result["verify_ok"]:
            result["error"] = f"verify failed: meta.end_date={meta.end_date if meta else None} < {latest_date.date()}"
            print(f"[{symbol}] ❌ Verify failed")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"[{symbol}] ❌ Exception: {e}")
        import traceback
        traceback.print_exc()
    
    return result


def main():
    print("=" * 60)
    print("US Stock Full Cycle Test")
    print("=" * 60)
    
    print("\nEnvironment check:")
    print(f"  SUPABASE_URL: {'SET' if os.getenv('SUPABASE_URL') else 'NOT SET'}")
    print(f"  SUPABASE_SERVICE_ROLE_KEY: {'SET' if os.getenv('SUPABASE_SERVICE_ROLE_KEY') else 'NOT SET'}")
    
    test_symbols = ["AAPL", "MSFT"]
    results = []
    
    for symbol in test_symbols:
        result = test_full_cycle(symbol)
        results.append(result)
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    for r in results:
        status = "✓" if r["verify_ok"] else "❌"
        print(f"{status} {r['symbol']}: download={r['download_ok']}, normalize={r['normalize_ok']}, upsert={r['upsert_ok']}, verify={r['verify_ok']}")
        if r["error"]:
            print(f"   Error: {r['error']}")
    
    success_count = sum(1 for r in results if r["verify_ok"])
    print(f"\nSuccess rate: {success_count}/{len(results)}")
    
    return 0 if success_count == len(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
