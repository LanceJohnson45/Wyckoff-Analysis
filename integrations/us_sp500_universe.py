from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import requests

from integrations.fetch_a_share_csv import _normalize_symbols


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "us_sp500_constituents.json"
_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_WIKI_API_URL = "https://en.wikipedia.org/w/api.php?action=parse&page=List_of_S%26P_500_companies&prop=text&formatversion=2&format=json"


@dataclass(frozen=True)
class UniverseSnapshot:
    source: str
    as_of: str
    symbols: list[str]


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


def _normalize_yahoo_symbol(raw: object) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return ""
    text = text.replace(".", "-")
    text = text.replace("/", "-")
    return text


def _snapshot_payload(symbols: list[str], *, source: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()
    return {
        "market": "us",
        "index": "sp500",
        "source": source,
        "as_of": today,
        "updated_at": now,
        "symbols": symbols,
    }


def snapshot_path() -> Path:
    return _SNAPSHOT_PATH


def load_sp500_snapshot() -> UniverseSnapshot | None:
    if not _SNAPSHOT_PATH.exists():
        return None
    try:
        payload = json.loads(_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    symbols = _normalize_symbols(
        [_normalize_yahoo_symbol(x) for x in payload.get("symbols", [])],
        market="us",
    )
    if not symbols:
        return None
    return UniverseSnapshot(
        source=str(payload.get("source", "snapshot") or "snapshot"),
        as_of=str(payload.get("as_of", "") or "").strip() or date.today().isoformat(),
        symbols=symbols,
    )


def save_sp500_snapshot(symbols: list[str], *, source: str) -> UniverseSnapshot:
    normalized = _normalize_symbols(
        [_normalize_yahoo_symbol(x) for x in symbols],
        market="us",
    )
    payload = _snapshot_payload(normalized, source=source)
    _atomic_json_dump(_SNAPSHOT_PATH, payload)
    return UniverseSnapshot(
        source=payload["source"],
        as_of=payload["as_of"],
        symbols=normalized,
    )


def fetch_sp500_constituents() -> UniverseSnapshot:
    try:
        resp = requests.get(_WIKI_API_URL, headers={"User-Agent": "Mozilla/5.0 (compatible; Wyckoff-Analysis/1.0)"}, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        html = str((payload.get("parse") or {}).get("text") or "")
        if not html:
            raise RuntimeError("MediaWiki parse API returned empty HTML")
        tables = pd.read_html(StringIO(html))
    except Exception as e:
        raise RuntimeError(f"failed to fetch S&P 500 constituents: {e}") from e
    if not tables:
        raise RuntimeError("S&P 500 constituents table not found")
    table = tables[0]
    if "Symbol" not in table.columns:
        raise RuntimeError("S&P 500 constituents table missing Symbol column")
    raw_symbols = table["Symbol"].astype(str).tolist()
    symbols = _normalize_symbols(
        [_normalize_yahoo_symbol(x) for x in raw_symbols],
        market="us",
    )
    if not symbols:
        raise RuntimeError("normalized S&P 500 universe is empty")
    return UniverseSnapshot(
        source="wikipedia_sp500",
        as_of=date.today().isoformat(),
        symbols=symbols,
    )


def get_sp500_constituents(*, prefer_snapshot: bool = True) -> UniverseSnapshot:
    if prefer_snapshot:
        snap = load_sp500_snapshot()
        if snap is not None:
            return snap
    return fetch_sp500_constituents()


def diff_symbols(previous: list[str], current: list[str]) -> tuple[list[str], list[str]]:
    prev = set(_normalize_symbols(previous, market="us"))
    curr = set(_normalize_symbols(current, market="us"))
    added = sorted(curr - prev)
    removed = sorted(prev - curr)
    return added, removed
