#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test Supabase connection and permissions."""

import os
import sys

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from integrations.supabase_base import create_admin_client
from core.constants import TABLE_STOCK_HIST_CACHE


def main():
    print("=" * 60)
    print("Supabase Connection Test")
    print("=" * 60)
    
    # Check environment variables
    print("\n1. Environment Variables:")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    print(f"   SUPABASE_URL: {'✓ Set' if url else '❌ Missing'}")
    print(f"   SUPABASE_KEY: {'✓ Set' if key else '❌ Missing'}")
    print(f"   SUPABASE_SERVICE_ROLE_KEY: {'✓ Set' if service_key else '❌ Missing'}")
    
    if not url or not service_key:
        print("\n❌ Required environment variables missing!")
        return 1
    
    # Test admin client creation
    print("\n2. Admin Client Creation:")
    try:
        client = create_admin_client()
        if client is None:
            print("   ❌ create_admin_client() returned None")
            return 1
        print("   ✓ Admin client created")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return 1
    
    # Test table access
    print("\n3. Table Access Test:")
    try:
        # Try to read one row
        resp = client.table(TABLE_STOCK_HIST_CACHE).select("symbol").limit(1).execute()
        print(f"   ✓ Can read from {TABLE_STOCK_HIST_CACHE}")
        print(f"   Rows returned: {len(resp.data)}")
    except Exception as e:
        print(f"   ❌ Read failed: {e}")
        return 1
    
    # Test write permission
    print("\n4. Write Permission Test:")
    test_record = {
        "symbol": "TEST:CONN",
        "adjust": "qfq",
        "date": "2026-01-01",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000000,
    }
    
    try:
        # Try upsert
        client.table(TABLE_STOCK_HIST_CACHE).upsert(test_record).execute()
        print("   ✓ Can write to table")
        
        # Verify write
        verify_resp = (
            client.table(TABLE_STOCK_HIST_CACHE)
            .select("symbol,date")
            .eq("symbol", "TEST:CONN")
            .eq("date", "2026-01-01")
            .execute()
        )
        
        if verify_resp.data:
            print("   ✓ Write verified")
        else:
            print("   ⚠️  Write succeeded but verification failed")
        
        # Cleanup
        client.table(TABLE_STOCK_HIST_CACHE).delete().eq("symbol", "TEST:CONN").execute()
        print("   ✓ Cleanup completed")
        
    except Exception as e:
        print(f"   ❌ Write test failed: {e}")
        return 1
    
    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
