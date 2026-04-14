#!/usr/bin/env python3
import os
import sys

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from integrations.supabase_base import create_admin_client, is_admin_configured
from core.constants import TABLE_STOCK_HIST_CACHE


def main():
    print("=" * 60)
    print("Supabase Connection Diagnostic")
    print("=" * 60)
    
    print("\n1. Environment Variables:")
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_KEY", "")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    
    print(f"   SUPABASE_URL: {'✓ Set (' + url[:20] + '...)' if url else '❌ Missing'}")
    print(f"   SUPABASE_KEY: {'✓ Set (len=' + str(len(key)) + ')' if key else '❌ Missing'}")
    print(f"   SUPABASE_SERVICE_ROLE_KEY: {'✓ Set (len=' + str(len(service_key)) + ')' if service_key else '❌ Missing'}")
    
    print(f"\n2. is_admin_configured(): {is_admin_configured()}")
    
    if not is_admin_configured():
        print("\n❌ Admin client not configured!")
        return 1
    
    print("\n3. Creating admin client...")
    try:
        client = create_admin_client()
        print("   ✓ Admin client created")
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n4. Testing table access...")
    try:
        resp = client.table(TABLE_STOCK_HIST_CACHE).select("symbol").limit(1).execute()
        print(f"   ✓ Can read from {TABLE_STOCK_HIST_CACHE}")
        print(f"   Rows: {len(resp.data)}")
    except Exception as e:
        print(f"   ❌ Read failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n5. Testing write...")
    test_record = {
        "symbol": "TEST:DIAG",
        "adjust": "qfq",
        "date": "2026-01-01",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000000,
    }
    
    try:
        client.table(TABLE_STOCK_HIST_CACHE).upsert(test_record).execute()
        print("   ✓ Write succeeded")
        
        verify_resp = (
            client.table(TABLE_STOCK_HIST_CACHE)
            .select("symbol,date")
            .eq("symbol", "TEST:DIAG")
            .eq("date", "2026-01-01")
            .execute()
        )
        
        if verify_resp.data:
            print("   ✓ Write verified")
        else:
            print("   ⚠️  Write succeeded but verification failed")
        
        client.table(TABLE_STOCK_HIST_CACHE).delete().eq("symbol", "TEST:DIAG").execute()
        print("   ✓ Cleanup completed")
        
    except Exception as e:
        print(f"   ❌ Write test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
