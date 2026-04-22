from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from core.constants import TABLE_INDEX_CONSTITUENTS_SNAPSHOT
from integrations.supabase_base import create_admin_client, is_admin_configured


@dataclass(frozen=True)
class IndexConstituentsSnapshot:
    market: str
    index_code: str
    as_of: str
    symbols: list[str]
    source: str
    raw_payload: dict[str, Any] | None = None


def load_index_constituents_snapshot(
    index_code: str,
    *,
    market: str = "hk",
) -> IndexConstituentsSnapshot | None:
    if not is_admin_configured():
        return None
    try:
        client = create_admin_client()
        resp = (
            client.table(TABLE_INDEX_CONSTITUENTS_SNAPSHOT)
            .select("market,index_code,as_of,symbols,source,raw_payload,updated_at")
            .eq("market", str(market or "").strip().lower())
            .eq("index_code", str(index_code or "").strip().lower())
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        row = resp.data[0] or {}
        symbols = row.get("symbols") or []
        if not isinstance(symbols, list):
            return None
        return IndexConstituentsSnapshot(
            market=str(row.get("market", "") or "").strip().lower(),
            index_code=str(row.get("index_code", "") or "").strip().lower(),
            as_of=str(row.get("as_of", "") or "").strip(),
            symbols=[str(x or "").strip() for x in symbols if str(x or "").strip()],
            source=str(row.get("source", "") or "").strip(),
            raw_payload=row.get("raw_payload")
            if isinstance(row.get("raw_payload"), dict)
            else None,
        )
    except Exception:
        return None


def upsert_index_constituents_snapshot(
    snapshot: IndexConstituentsSnapshot,
) -> bool:
    if not is_admin_configured():
        return False
    try:
        client = create_admin_client()
        payload = {
            "market": str(snapshot.market or "").strip().lower(),
            "index_code": str(snapshot.index_code or "").strip().lower(),
            "as_of": str(snapshot.as_of or "").strip(),
            "symbols": [str(x or "").strip() for x in snapshot.symbols if str(x or "").strip()],
            "source": str(snapshot.source or "").strip(),
            "raw_payload": snapshot.raw_payload if isinstance(snapshot.raw_payload, dict) else None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        (
            client.table(TABLE_INDEX_CONSTITUENTS_SNAPSHOT)
            .upsert(payload, on_conflict="market,index_code")
            .execute()
        )
        return True
    except Exception:
        return False
