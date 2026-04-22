from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from typing import Any

from integrations.supabase_index_constituents import (
    IndexConstituentsSnapshot,
    load_index_constituents_snapshot,
    upsert_index_constituents_snapshot,
)


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "hk_hsi_hstech_constituents.json"
_HSI_DATA_PATH = _DATA_DIR / "hsi_data.json"
_HSTECH_DATA_PATH = _DATA_DIR / "hstech_data.json"
_HK_EXCLUDE_SYMBOLS = {"0000.HK"}


@dataclass(frozen=True)
class HkUniverseSnapshot:
    source: str
    as_of: str
    symbols: list[str]
    hsi_symbols: list[str]
    hstech_symbols: list[str]
    raw_payloads: dict[str, Any] | None = None


def _atomic_json_dump(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name: str | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=str(path.parent),
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as tmp:
            json.dump(payload, tmp, ensure_ascii=False, indent=2)
            tmp.flush()
            tmp_name = tmp.name
        Path(tmp_name).replace(path)
        tmp_name = None
    finally:
        if tmp_name:
            try:
                Path(tmp_name).unlink(missing_ok=True)
            except Exception:
                pass


def _normalize_hk_symbol(raw: object) -> str:
    text = str(raw or "").strip().upper()
    if text.endswith(".HK"):
        text = text[:-3]
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    symbol = f"{digits[-4:].zfill(4)}.HK"
    if symbol in _HK_EXCLUDE_SYMBOLS:
        return ""
    return symbol


def _normalize_symbols(symbols: list[str], *, market: str = "hk") -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    market_norm = str(market or "hk").strip().lower()
    for raw in symbols:
        s = str(raw).strip()
        if not s:
            continue
        if market_norm == "hk":
            s = s.upper()
            if s.endswith(".HK"):
                s = s[:-3]
            digits = "".join(ch for ch in s if ch.isdigit())
            if not digits:
                continue
            s = f"{digits[-4:].zfill(4)}.HK"
        elif market_norm == "us":
            s = s.upper()
            if not re.fullmatch(r"[A-Z][A-Z0-9._-]{0,14}", s):
                continue
        else:
            if not re.fullmatch(r"\d{6}", s):
                continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise RuntimeError(f"missing local HK constituent file: {path.name}") from e
    except Exception as e:
        raise RuntimeError(f"failed to read local HK constituent file {path.name}: {e}") from e


def _parse_components_from_local_rows(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        raise RuntimeError("HK constituent file payload must be a JSON array")
    seen: set[str] = set()
    symbols: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = _normalize_hk_symbol(row.get("code"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    normalized = _normalize_symbols(symbols, market="hk")
    if not normalized:
        raise RuntimeError("failed to parse HK local constituent payload")
    return normalized


def _resolve_as_of(*paths: Path) -> str:
    latest_ts = 0.0
    for path in paths:
        try:
            latest_ts = max(latest_ts, path.stat().st_mtime)
        except Exception:
            continue
    if latest_ts > 0:
        return datetime.fromtimestamp(latest_ts, tz=timezone.utc).date().isoformat()
    return date.today().isoformat()


def fetch_hk_index_union() -> HkUniverseSnapshot:
    hsi_payload = _load_json_file(_HSI_DATA_PATH)
    hstech_payload = _load_json_file(_HSTECH_DATA_PATH)
    hsi_symbols = _parse_components_from_local_rows(hsi_payload)
    hstech_symbols = _parse_components_from_local_rows(hstech_payload)
    union = _normalize_symbols(hsi_symbols + hstech_symbols, market="hk")
    return HkUniverseSnapshot(
        source="local_data_hsi_hstech",
        as_of=_resolve_as_of(_HSI_DATA_PATH, _HSTECH_DATA_PATH),
        symbols=union,
        hsi_symbols=hsi_symbols,
        hstech_symbols=hstech_symbols,
        raw_payloads={"hsi": hsi_payload, "hstech": hstech_payload},
    )


def load_hk_snapshot() -> HkUniverseSnapshot | None:
    hsi_db = load_index_constituents_snapshot("hsi", market="hk")
    hstech_db = load_index_constituents_snapshot("hstech", market="hk")
    if hsi_db is not None and hstech_db is not None and hsi_db.symbols and hstech_db.symbols:
        symbols = _normalize_symbols(hsi_db.symbols + hstech_db.symbols, market="hk")
        if symbols:
            return HkUniverseSnapshot(
                source=f"{hsi_db.source}+{hstech_db.source}",
                as_of=max(str(hsi_db.as_of or ""), str(hstech_db.as_of or "")),
                symbols=symbols,
                hsi_symbols=_normalize_symbols(hsi_db.symbols, market="hk"),
                hstech_symbols=_normalize_symbols(hstech_db.symbols, market="hk"),
                raw_payloads={
                    "hsi": hsi_db.raw_payload or {},
                    "hstech": hstech_db.raw_payload or {},
                },
            )
    if not _SNAPSHOT_PATH.exists():
        return None
    try:
        payload = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    symbols = _normalize_symbols(payload.get("symbols", []), market="hk")
    hsi_symbols = _normalize_symbols(payload.get("hsi_symbols", []), market="hk")
    hstech_symbols = _normalize_symbols(payload.get("hstech_symbols", []), market="hk")
    if not symbols:
        return None
    return HkUniverseSnapshot(
        source=str(payload.get("source", "snapshot") or "snapshot"),
        as_of=str(payload.get("as_of", "") or "").strip() or date.today().isoformat(),
        symbols=symbols,
        hsi_symbols=hsi_symbols,
        hstech_symbols=hstech_symbols,
        raw_payloads=None,
    )


def save_hk_snapshot(snapshot: HkUniverseSnapshot) -> HkUniverseSnapshot:
    payload = {
        "market": "hk",
        "pool": "hsi_hstech_union",
        "source": snapshot.source,
        "as_of": snapshot.as_of,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": snapshot.symbols,
        "hsi_symbols": snapshot.hsi_symbols,
        "hstech_symbols": snapshot.hstech_symbols,
    }
    _atomic_json_dump(_SNAPSHOT_PATH, payload)
    raw_payloads = snapshot.raw_payloads or {}
    upsert_index_constituents_snapshot(
        IndexConstituentsSnapshot(
            market="hk",
            index_code="hsi",
            as_of=snapshot.as_of,
            symbols=list(snapshot.hsi_symbols),
            source=snapshot.source,
            raw_payload=raw_payloads.get("hsi")
            if isinstance(raw_payloads.get("hsi"), (dict, list))
            else None,
        )
    )
    upsert_index_constituents_snapshot(
        IndexConstituentsSnapshot(
            market="hk",
            index_code="hstech",
            as_of=snapshot.as_of,
            symbols=list(snapshot.hstech_symbols),
            source=snapshot.source,
            raw_payload=raw_payloads.get("hstech")
            if isinstance(raw_payloads.get("hstech"), (dict, list))
            else None,
        )
    )
    return snapshot


def get_hk_index_union(*, prefer_snapshot: bool = True) -> HkUniverseSnapshot:
    cached = load_hk_snapshot()
    try:
        fresh = fetch_hk_index_union()
    except Exception as e:
        if cached is not None:
            return HkUniverseSnapshot(
                source=f"{cached.source}|stale_snapshot_fallback:{type(e).__name__}",
                as_of=str(cached.as_of or ""),
                symbols=list(cached.symbols),
                hsi_symbols=list(cached.hsi_symbols),
                hstech_symbols=list(cached.hstech_symbols),
                raw_payloads=cached.raw_payloads,
            )
        raise RuntimeError(
            "failed to load HK constituents from local data files and no snapshot is available"
        ) from e
    if not prefer_snapshot or cached is None or cached.symbols != fresh.symbols:
        save_hk_snapshot(fresh)
    return fresh


def diff_symbols(previous: list[str], current: list[str]) -> tuple[list[str], list[str]]:
    prev = set(_normalize_symbols(previous, market="hk"))
    curr = set(_normalize_symbols(current, market="hk"))
    return sorted(curr - prev), sorted(prev - curr)


def snapshot_path() -> Path:
    return _SNAPSHOT_PATH
