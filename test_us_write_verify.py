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


def test_single_write_verify(symbol: str) -> dict:
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
        latest_date = pd.to_datetime(norm["date"], errors="coerce").dropna().max()
        print(f"[{symbol}] ✓ Normalized to {len(norm)} rows")
        print(f"[{symbol}]   Latest date: {latest_date.date()}")
        
        print(f"[{symbol}] Step 3: Upsert with US: prefix")
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
        
        print(f"[{symbol}] Step 4: Immediate verify (no delay)")
        meta = get_cache_meta(f"US:{symbol}", "qfq", context="background")
        
        if meta is None:
            result["error"] = "meta is None immediately after upsert"
            print(f"[{symbol}] ❌ Meta is None")
            return result
        
        print(f"[{symbol}]   Meta: start={meta.start_date}, end={meta.end_date}")
        print(f"[{symbol}]   Expected end: {latest_date.date()}")
        
        if meta.end_date < latest_date.date():
            result["error"] = f"meta.end_date {meta.end_date} < expected {latest_date.date()}"
            print(f"[{symbol}] ❌ End date mismatch")
            
            print(f"[{symbol}] Retrying with delays...")
            for delay in [0.5, 1.0, 2.0]:
                time.sleep(delay)
                meta = get_cache_meta(f"US:{symbol}", "qfq", context="background")
                print(f"[{symbol}]   After {delay}s: end={meta.end_date if meta else None}")
                if meta and meta.end_date >= latest_date.date():
                    result["verify_ok"] = True
                    print(f"[{symbol}] ✓ Verify passed after {delay}s")
                    break
        else:
            result["verify_ok"] = True
            print(f"[{symbol}] ✓ Verify passed immediately")
        
        if not result["verify_ok"]:
            result["error"] = f"verify failed after retries: meta.end_date={meta.end_date if meta else None}"
        
    except Exception as e:
        result["error"] = str(e)
        print(f"[{symbol}] ❌ Exception: {e}")
        import traceback
        traceback.print_exc()
    
    return result


def main():
    print("=" * 60)
    print("US Stock Write/Verify Test (with US: prefix)")
    print("=" * 60)
    
    test_symbols = ["AAPL", "MSFT"]
    results = []
    
    for symbol in test_symbols:
        result = test_single_write_verify(symbol)
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
