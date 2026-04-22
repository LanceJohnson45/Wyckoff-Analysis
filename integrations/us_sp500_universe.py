from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from typing import Any


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "us_sp500_constituents.json"
_INPUT_PATHS = {
    "dowjones": _DATA_DIR / "dowjones.json",
    "sp500": _DATA_DIR / "sp500.json",
    "qqq": _DATA_DIR / "qqq.json",
}


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


def _normalize_symbols(symbols: list[str], *, market: str = "us") -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    market_norm = str(market or "us").strip().lower()
    for raw in symbols:
        s = str(raw).strip()
        if not s:
            continue
        if market_norm == "us":
            s = s.upper()
            if not re.fullmatch(r"[A-Z][A-Z0-9._-]{0,14}", s):
                continue
        elif market_norm == "hk":
            s = s.upper()
            if s.endswith(".HK"):
                s = s[:-3]
            digits = "".join(ch for ch in s if ch.isdigit())
            if not digits:
                continue
            s = f"{digits[-4:].zfill(4)}.HK"
        else:
            if not re.fullmatch(r"\d{6}", s):
                continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _snapshot_payload(symbols: list[str], *, source: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    today = date.today().isoformat()
    return {
        "market": "us",
        "index": "dowjones_sp500_qqq_union",
        "source": source,
        "as_of": today,
        "updated_at": now,
        "symbols": symbols,
    }


def _load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise RuntimeError(f"missing local US constituent file: {path.name}") from e
    except Exception as e:
        raise RuntimeError(f"failed to read local US constituent file {path.name}: {e}") from e


def _parse_symbols_from_local_rows(rows: Any) -> list[str]:
    if not isinstance(rows, list):
        raise RuntimeError("US constituent file payload must be a JSON array")
    seen: set[str] = set()
    symbols: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = _normalize_yahoo_symbol(row.get("symbol"))
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    normalized = _normalize_symbols(symbols, market="us")
    if not normalized:
        raise RuntimeError("failed to parse US local constituent payload")
    return normalized


def _resolve_as_of(paths: list[Path]) -> str:
    latest_ts = 0.0
    for path in paths:
        try:
            latest_ts = max(latest_ts, path.stat().st_mtime)
        except Exception:
            continue
    if latest_ts > 0:
        return datetime.fromtimestamp(latest_ts, tz=timezone.utc).date().isoformat()
    return date.today().isoformat()


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
    all_symbols: list[str] = []
    input_paths = list(_INPUT_PATHS.values())
    for path in input_paths:
        payload = _load_json_file(path)
        all_symbols.extend(_parse_symbols_from_local_rows(payload))
    symbols = _normalize_symbols(all_symbols, market="us")
    if not symbols:
        raise RuntimeError("normalized US local universe is empty")
    return UniverseSnapshot(
        source="local_data_dowjones_sp500_qqq",
        as_of=_resolve_as_of(input_paths),
        symbols=symbols,
    )


def get_sp500_constituents(*, prefer_snapshot: bool = True) -> UniverseSnapshot:
    cached = load_sp500_snapshot()
    try:
        fresh = fetch_sp500_constituents()
    except Exception as e:
        if cached is not None:
            return UniverseSnapshot(
                source=f"{cached.source}|stale_snapshot_fallback:{type(e).__name__}",
                as_of=str(cached.as_of or ""),
                symbols=list(cached.symbols),
            )
        raise RuntimeError(
            "failed to load US constituents from local data files and no snapshot is available"
        ) from e
    if not prefer_snapshot or cached is None or cached.symbols != fresh.symbols:
        save_sp500_snapshot(fresh.symbols, source=fresh.source)
    return fresh


def diff_symbols(previous: list[str], current: list[str]) -> tuple[list[str], list[str]]:
    prev = set(_normalize_symbols(previous, market="us"))
    curr = set(_normalize_symbols(current, market="us"))
    added = sorted(curr - prev)
    removed = sorted(prev - curr)
    return added, removed
