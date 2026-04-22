from __future__ import annotations

import pytest

from integrations import hk_index_universe as mod
from integrations.hk_index_universe import (
    HkUniverseSnapshot,
    _extract_symbol_map_from_html,
    _normalize_hk_symbol,
    _normalize_name,
)


def test_normalize_hk_symbol_zero_pads_and_suffixes():
    assert _normalize_hk_symbol("700") == "0700.HK"
    assert _normalize_hk_symbol("0700.HK") == "0700.HK"
    assert _normalize_hk_symbol("9988") == "9988.HK"
    assert _normalize_hk_symbol("0000") == ""


def test_extract_symbol_map_from_html_reads_embedded_json():
    html = '{"flag":{"name":"Tencent Holdings","code":"HK"},"name":{"label":"Tencent Holdings","title":"Tencent Holdings Ltd","derived":false,"url":"/equities/tencent-holdings-hk"},"symbol":"0700","ticker":"","exchangeId":"21"}'
    mapping = _extract_symbol_map_from_html(html)
    assert mapping[_normalize_name("Tencent Holdings")] == "0700.HK"


def test_get_hk_index_union_falls_back_to_snapshot_when_fetch_fails(monkeypatch):
    cached = HkUniverseSnapshot(
        source="snapshot",
        as_of="2026-04-22",
        symbols=["0700.HK", "9988.HK"],
        hsi_symbols=["0700.HK"],
        hstech_symbols=["9988.HK"],
    )

    monkeypatch.setattr(mod, "load_hk_snapshot", lambda: cached)

    def _raise() -> HkUniverseSnapshot:
        raise RuntimeError("403 forbidden")

    monkeypatch.setattr(mod, "fetch_hk_index_union", _raise)
    snapshot = mod.get_hk_index_union(prefer_snapshot=False)
    assert snapshot.symbols == cached.symbols
    assert "stale_snapshot_fallback" in snapshot.source


def test_get_hk_index_union_raises_when_no_snapshot_and_fetch_fails(monkeypatch):
    monkeypatch.setattr(mod, "load_hk_snapshot", lambda: None)

    def _raise() -> HkUniverseSnapshot:
        raise RuntimeError("403 forbidden")

    monkeypatch.setattr(mod, "fetch_hk_index_union", _raise)
    with pytest.raises(RuntimeError, match="no local snapshot"):
        mod.get_hk_index_union(prefer_snapshot=False)
