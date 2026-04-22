from __future__ import annotations

import pytest

from integrations import hk_index_universe as hk_mod
from integrations import us_sp500_universe as us_mod
from integrations.hk_index_universe import (
    HkUniverseSnapshot,
    _normalize_hk_symbol,
    _parse_components_from_local_rows,
)


def test_normalize_hk_symbol_zero_pads_and_suffixes():
    assert _normalize_hk_symbol("700") == "0700.HK"
    assert _normalize_hk_symbol("0700.HK") == "0700.HK"
    assert _normalize_hk_symbol("9988") == "9988.HK"
    assert _normalize_hk_symbol("0000") == ""


def test_parse_components_from_local_rows_extracts_hk_codes():
    rows = [
        {"code": "700", "constituentName": "腾讯控股"},
        {"code": "241", "constituentName": "阿里健康"},
        {"code": "1024", "constituentName": "快手 - W"},
        {"code": "700", "constituentName": "腾讯控股"},
    ]
    symbols = _parse_components_from_local_rows(rows)
    assert symbols == ["0700.HK", "0241.HK", "1024.HK"]


def test_get_hk_index_union_reads_local_data_first(monkeypatch):
    fresh = HkUniverseSnapshot(
        source="local_data_hsi_hstech",
        as_of="2026-04-22",
        symbols=["0700.HK", "9988.HK"],
        hsi_symbols=["0700.HK"],
        hstech_symbols=["9988.HK"],
    )
    cached = HkUniverseSnapshot(
        source="snapshot",
        as_of="2026-04-01",
        symbols=["0005.HK"],
        hsi_symbols=["0005.HK"],
        hstech_symbols=[],
    )

    monkeypatch.setattr(hk_mod, "fetch_hk_index_union", lambda: fresh)
    monkeypatch.setattr(hk_mod, "load_hk_snapshot", lambda: cached)
    saved: list[HkUniverseSnapshot] = []
    monkeypatch.setattr(hk_mod, "save_hk_snapshot", lambda snap: saved.append(snap) or snap)

    snapshot = hk_mod.get_hk_index_union(prefer_snapshot=True)
    assert snapshot.symbols == fresh.symbols
    assert saved and saved[0].symbols == fresh.symbols


def test_get_hk_index_union_falls_back_to_snapshot_when_local_data_fails(monkeypatch):
    cached = HkUniverseSnapshot(
        source="snapshot",
        as_of="2026-04-22",
        symbols=["0700.HK", "9988.HK"],
        hsi_symbols=["0700.HK"],
        hstech_symbols=["9988.HK"],
    )

    monkeypatch.setattr(hk_mod, "load_hk_snapshot", lambda: cached)

    def _raise() -> HkUniverseSnapshot:
        raise RuntimeError("missing local files")

    monkeypatch.setattr(hk_mod, "fetch_hk_index_union", _raise)
    snapshot = hk_mod.get_hk_index_union(prefer_snapshot=False)
    assert snapshot.symbols == cached.symbols
    assert "stale_snapshot_fallback" in snapshot.source


def test_get_hk_index_union_raises_when_no_snapshot_and_local_data_fails(monkeypatch):
    monkeypatch.setattr(hk_mod, "load_hk_snapshot", lambda: None)

    def _raise() -> HkUniverseSnapshot:
        raise RuntimeError("missing local files")

    monkeypatch.setattr(hk_mod, "fetch_hk_index_union", _raise)
    with pytest.raises(RuntimeError, match="local data files"):
        hk_mod.get_hk_index_union(prefer_snapshot=False)


def test_us_local_union_combines_three_inputs(monkeypatch):
    monkeypatch.setattr(
        us_mod,
        "_load_json_file",
        lambda path: [{"symbol": "MSFT"}, {"symbol": "AAPL"}]
        if path.name == "dowjones.json"
        else ([{"symbol": "MSFT"}, {"symbol": "NVDA"}] if path.name == "sp500.json" else [{"symbol": "QQQ"}, {"symbol": "NVDA"}]),
    )

    snapshot = us_mod.fetch_sp500_constituents()
    assert snapshot.source == "local_data_dowjones_sp500_qqq"
    assert snapshot.symbols == ["MSFT", "AAPL", "NVDA", "QQQ"]


def test_get_us_union_reads_local_data_first(monkeypatch):
    fresh = us_mod.UniverseSnapshot(
        source="local_data_dowjones_sp500_qqq",
        as_of="2026-04-22",
        symbols=["AAPL", "MSFT"],
    )
    cached = us_mod.UniverseSnapshot(
        source="snapshot",
        as_of="2026-04-01",
        symbols=["IBM"],
    )
    monkeypatch.setattr(us_mod, "fetch_sp500_constituents", lambda: fresh)
    monkeypatch.setattr(us_mod, "load_sp500_snapshot", lambda: cached)
    saved: list[tuple[list[str], str]] = []
    monkeypatch.setattr(
        us_mod,
        "save_sp500_snapshot",
        lambda symbols, source: saved.append((symbols, source)) or fresh,
    )

    snapshot = us_mod.get_sp500_constituents(prefer_snapshot=True)
    assert snapshot.symbols == fresh.symbols
    assert saved and saved[0][0] == fresh.symbols


def test_get_us_union_falls_back_to_snapshot_when_local_data_fails(monkeypatch):
    cached = us_mod.UniverseSnapshot(
        source="snapshot",
        as_of="2026-04-22",
        symbols=["AAPL", "MSFT"],
    )
    monkeypatch.setattr(us_mod, "load_sp500_snapshot", lambda: cached)

    def _raise() -> us_mod.UniverseSnapshot:
        raise RuntimeError("missing local files")

    monkeypatch.setattr(us_mod, "fetch_sp500_constituents", _raise)
    snapshot = us_mod.get_sp500_constituents(prefer_snapshot=False)
    assert snapshot.symbols == cached.symbols
    assert "stale_snapshot_fallback" in snapshot.source
