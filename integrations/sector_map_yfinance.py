from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from integrations.fetch_a_share_csv import get_stocks_by_board
from integrations.hk_index_universe import get_hk_index_union
from integrations.us_sp500_universe import get_sp500_constituents


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_CACHE_PATH = _DATA_DIR / "yahoo_sector_map_cache.json"
_LEGACY_CACHE_PATH = _DATA_DIR / "sector_map_cache.json"


@dataclass(frozen=True)
class YahooSectorRecord:
    symbol: str
    market: str
    sector: str
    sector_key: str
    industry: str
    industry_key: str
    source: str
    updated_at: str


def cache_path() -> Path:
    return _CACHE_PATH


def legacy_cache_path() -> Path:
    return _LEGACY_CACHE_PATH


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


def _cn_to_yfinance_symbol(code: str) -> str:
    text = str(code or "").strip()
    if len(text) != 6 or not text.isdigit():
        return ""
    if text.startswith(("600", "601", "603", "605", "688")):
        return f"{text}.SS"
    if text.startswith(("000", "001", "002", "003", "300", "301")):
        return f"{text}.SZ"
    return ""


def _normalize_target_symbol(symbol: str, market: str) -> str:
    market_norm = str(market or "").strip().lower()
    text = str(symbol or "").strip().upper()
    if not text:
        return ""
    if market_norm == "cn":
        if text.endswith((".SS", ".SZ")):
            return text
        return _cn_to_yfinance_symbol(text)
    if market_norm == "hk":
        if text.endswith(".HK"):
            return text
        digits = "".join(ch for ch in text if ch.isdigit())
        return f"{digits[-4:].zfill(4)}.HK" if digits else ""
    if market_norm == "us":
        return text.replace(".", "-").replace("/", "-")
    return ""


def _all_targets_for_market(market: str) -> list[str]:
    market_norm = str(market or "").strip().lower()
    if market_norm == "us":
        return list(get_sp500_constituents(prefer_snapshot=True).symbols)
    if market_norm == "hk":
        return list(get_hk_index_union(prefer_snapshot=True).symbols)
    if market_norm == "cn":
        items = get_stocks_by_board("main") + get_stocks_by_board("chinext")
        seen: set[str] = set()
        out: list[str] = []
        for item in items:
            code = str(item.get("code", "")).strip()
            yf_symbol = _cn_to_yfinance_symbol(code)
            if yf_symbol and yf_symbol not in seen:
                seen.add(yf_symbol)
                out.append(yf_symbol)
        return out
    return []


def load_sector_cache() -> dict[str, dict[str, Any]]:
    if not _CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    items = payload.get("items", {}) if isinstance(payload, dict) else {}
    if not isinstance(items, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for symbol, row in items.items():
        if not isinstance(row, dict):
            continue
        out[str(symbol).strip().upper()] = dict(row)
    return out


def save_sector_cache(items: dict[str, dict[str, Any]]) -> None:
    payload = {
        "source": "yfinance_sector_industry",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "items": items,
    }
    _atomic_json_dump(_CACHE_PATH, payload)


def clear_legacy_sector_cache() -> bool:
    try:
        _LEGACY_CACHE_PATH.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def sector_map_from_cache() -> dict[str, str]:
    return classification_map_from_cache(field="sector")


def industry_map_from_cache() -> dict[str, str]:
    return classification_map_from_cache(field="industry")


def classification_map_from_cache(*, field: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for symbol, row in load_sector_cache().items():
        market = str(row.get("market", "") or "").strip().lower()
        value = str(row.get(field, "") or "").strip()
        if not value:
            continue
        if market == "cn":
            out[symbol.split(".")[0]] = value
        else:
            out[symbol] = value
    return out


def missing_symbols_from_cache(symbols: list[str], market: str) -> list[str]:
    cache = load_sector_cache()
    market_norm = str(market or "").strip().lower()
    out: list[str] = []
    for symbol in symbols:
        yf_symbol = _normalize_target_symbol(symbol, market_norm)
        if not yf_symbol:
            continue
        row = cache.get(yf_symbol)
        sector = str((row or {}).get("sector", "") or "").strip()
        if not sector:
            out.append(yf_symbol)
    return out


def _probe_symbol(yf, symbol: str, market: str) -> dict[str, Any]:
    ticker = yf.Ticker(symbol)
    info = ticker.info
    if not isinstance(info, dict):
        info = {}
    return {
        "symbol": symbol,
        "market": market,
        "sector": str(info.get("sector") or "").strip(),
        "sector_key": str(info.get("sectorKey") or "").strip(),
        "industry": str(info.get("industry") or "").strip(),
        "industry_key": str(info.get("industryKey") or "").strip(),
        "source": "yfinance.info",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def refresh_sector_cache(
    *,
    markets: list[str],
    delay_seconds: float = 1.2,
    retries: int = 1,
    max_symbols_per_market: int = 0,
    force: bool = False,
    checkpoint_every: int = 20,
) -> dict[str, Any]:
    import yfinance as yf

    delay = max(float(delay_seconds), 0.0)
    retry_count = max(int(retries), 0)
    cache = load_sector_cache()
    checkpoint_size = max(int(checkpoint_every), 1)
    stats: dict[str, Any] = {
        "markets": {},
        "legacy_cache_cleared": clear_legacy_sector_cache(),
        "checkpoint_every": checkpoint_size,
    }
    for market in markets:
        market_norm = str(market or "").strip().lower()
        targets = _all_targets_for_market(market_norm)
        if max_symbols_per_market > 0:
            targets = targets[: max_symbols_per_market]
        yf_symbols = [_normalize_target_symbol(sym, market_norm) for sym in targets]
        yf_symbols = [sym for sym in yf_symbols if sym]
        if not force:
            pending = missing_symbols_from_cache(yf_symbols, market_norm)
        else:
            pending = yf_symbols
        market_stats = {
            "targets": len(yf_symbols),
            "pending": len(pending),
            "ok": 0,
            "failed": 0,
            "errors": {},
            "checkpoints": 0,
        }
        for idx, symbol in enumerate(pending, start=1):
            last_err = ""
            for attempt in range(retry_count + 1):
                try:
                    row = _probe_symbol(yf, symbol, market_norm)
                    cache[symbol] = row
                    market_stats["ok"] += 1
                    last_err = ""
                    break
                except Exception as exc:
                    last_err = f"{type(exc).__name__}: {exc}"
                    if attempt < retry_count:
                        time.sleep(delay)
            if last_err:
                market_stats["failed"] += 1
                market_stats["errors"][symbol] = last_err
            if idx % checkpoint_size == 0:
                save_sector_cache(cache)
                market_stats["checkpoints"] += 1
            if delay > 0 and idx < len(pending):
                time.sleep(delay)
        save_sector_cache(cache)
        market_stats["checkpoints"] += 1
        stats["markets"][market_norm] = market_stats
    return stats
