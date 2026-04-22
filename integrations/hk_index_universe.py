from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

import pandas as pd
import requests

from integrations.fetch_a_share_csv import _normalize_symbols


_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_SNAPSHOT_PATH = _DATA_DIR / "hk_hsi_hstech_constituents.json"
_HSI_COMPONENTS_URL = "https://www.investing.com/indices/hang-sen-40-components"
_HSTECH_COMPONENTS_URL = "https://www.investing.com/indices/hang-seng-tech-components"
_USER_AGENT = "Mozilla/5.0 (compatible; Wyckoff-Analysis/1.0)"
_YAHOO_SEARCH_URL = "https://query2.finance.yahoo.com/v1/finance/search"
_HK_NAME_ALIASES = {
    "ALIBABA": "9988.HK",
    "BAIDU": "9888.HK",
    "BILIBILI": "9626.HK",
    "BYDCOLTDH": "1211.HK",
    "HAIERSMARTHOMECO": "6690.HK",
    "HORIZONROBOTICS": "9660.HK",
    "JD": "9618.HK",
    "JDHEALTH": "6618.HK",
    "KINGDEEINTSOFTWARE": "0268.HK",
    "KUAISHOUTECHNOLOGY": "1024.HK",
    "LIAUTO": "2015.HK",
    "MEITUAN": "3690.HK",
    "MIDEAGROUPH": "0300.HK",
    "NETEASE": "9999.HK",
    "NIO": "9866.HK",
    "SENSETIMEGROUPINCB": "0020.HK",
    "SMIC": "0981.HK",
    "SUNNYOPTICALTECH": "2382.HK",
    "TENCENTHOLDINGS": "0700.HK",
    "TENCENTMUSICENTERTAINMENT": "1698.HK",
    "TONGCHENGELONG": "0780.HK",
    "TRIPCOMGROUP": "9961.HK",
    "XIAOMI": "1810.HK",
    "XPENG": "9868.HK",
}
_HK_EXCLUDE_SYMBOLS = {
    "0000.HK",
}


@dataclass(frozen=True)
class HkUniverseSnapshot:
    source: str
    as_of: str
    symbols: list[str]
    hsi_symbols: list[str]
    hstech_symbols: list[str]


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


def _normalize_name(raw: object) -> str:
    text = str(raw or "").strip().upper()
    return re.sub(r"[^A-Z0-9]+", "", text)


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


def _load_tables(url: str) -> tuple[str, list[pd.DataFrame]]:
    resp = requests.get(url, headers={"User-Agent": _USER_AGENT}, timeout=30)
    resp.raise_for_status()
    html = resp.text
    tables = pd.read_html(StringIO(html))
    return html, tables


def _extract_symbol_map_from_html(html: str) -> dict[str, str]:
    pattern = re.compile(
        r'"flag":\{"name":"(?P<flag>[^\"]+)","code":"HK"\},'
        r'"name":\{"label":"(?P<label>[^\"]+)","title":"(?P<title>[^\"]+)".*?'
        r'"url":"(?P<url>/equities/[^\"]+)".*?'
        r'"symbol":"(?P<symbol>\d{1,5})","ticker":"","exchangeId":"21"',
        flags=re.DOTALL,
    )
    out: dict[str, str] = {}
    for match in pattern.finditer(html):
        symbol = _normalize_hk_symbol(match.group("symbol"))
        if not symbol:
            continue
        url_key = str(match.group("url") or "").strip()
        if url_key and url_key not in out:
            out[url_key] = symbol
        for key in {match.group("flag"), match.group("label"), match.group("title")}:
            norm_key = _normalize_name(key)
            if norm_key and norm_key not in out:
                out[norm_key] = symbol
    return out


def _select_name_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    candidates: list[pd.DataFrame] = []
    for table in tables:
        cols = {str(c).strip().lower() for c in table.columns}
        if "name" in cols and len(table.index) >= 20:
            candidates.append(table)
    if not candidates:
        raise RuntimeError("component name table not found")
    return max(candidates, key=lambda x: len(x.index)).copy()


def _parse_components(url: str) -> list[str]:
    html, tables = _load_tables(url)
    name_table = _select_name_table(tables)
    symbol_map = _extract_symbol_map_from_html(html)
    symbols: list[str] = []
    seen: set[str] = set()
    for value in name_table.get("Name", []).tolist():
        name = value[0] if isinstance(value, tuple) else value
        href = value[1] if isinstance(value, tuple) and len(value) > 1 else None
        symbol = ""
        if href:
            symbol = symbol_map.get(str(href).strip(), "")
        if not symbol:
            symbol = symbol_map.get(_normalize_name(name), "")
        if not symbol:
            symbol = _HK_NAME_ALIASES.get(_normalize_name(name), "")
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    normalized = _normalize_symbols(symbols, market="hk")
    if not normalized:
        raise RuntimeError(f"failed to parse HK components from {url}")
    return normalized


def _resolve_yahoo_hk_symbol(name: str) -> str:
    norm_name = _normalize_name(name)
    if not norm_name:
        return ""
    if norm_name in _HK_NAME_ALIASES:
        return _HK_NAME_ALIASES[norm_name]
    try:
        resp = requests.get(
            _YAHOO_SEARCH_URL,
            params={"q": name, "quotesCount": 10, "newsCount": 0},
            headers={"User-Agent": _USER_AGENT},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return ""
    candidates = payload.get("quotes", []) or []
    for item in candidates:
        symbol = _normalize_hk_symbol(item.get("symbol"))
        exchange = str(item.get("exchange") or "").strip().upper()
        if symbol and exchange == "HKG":
            return symbol
    for item in candidates:
        symbol = _normalize_hk_symbol(item.get("symbol"))
        if symbol:
            return symbol
    return ""


def fetch_hk_index_union() -> HkUniverseSnapshot:
    hsi_symbols = _parse_components(_HSI_COMPONENTS_URL)
    hstech_symbols = _parse_components(_HSTECH_COMPONENTS_URL)
    union = _normalize_symbols(hsi_symbols + hstech_symbols, market="hk")
    return HkUniverseSnapshot(
        source="investing_hsi_hstech_components",
        as_of=date.today().isoformat(),
        symbols=union,
        hsi_symbols=hsi_symbols,
        hstech_symbols=hstech_symbols,
    )


def load_hk_snapshot() -> HkUniverseSnapshot | None:
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
    return snapshot


def get_hk_index_union(*, prefer_snapshot: bool = True) -> HkUniverseSnapshot:
    cached = load_hk_snapshot()
    if prefer_snapshot and cached is not None:
        return cached
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
            )
        raise RuntimeError(
            "failed to fetch HK HSI/HSTECH constituents and no local snapshot is available"
        ) from e
    save_hk_snapshot(fresh)
    return fresh


def diff_symbols(previous: list[str], current: list[str]) -> tuple[list[str], list[str]]:
    prev = set(_normalize_symbols(previous, market="hk"))
    curr = set(_normalize_symbols(current, market="hk"))
    return sorted(curr - prev), sorted(prev - curr)


def snapshot_path() -> Path:
    return _SNAPSHOT_PATH
