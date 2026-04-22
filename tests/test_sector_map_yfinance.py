from __future__ import annotations

from integrations import sector_map_yfinance as mod


def test_normalize_target_symbol_by_market():
    assert mod._normalize_target_symbol("600519", "cn") == "600519.SS"
    assert mod._normalize_target_symbol("000001", "cn") == "000001.SZ"
    assert mod._normalize_target_symbol("0700", "hk") == "0700.HK"
    assert mod._normalize_target_symbol("0700.HK", "hk") == "0700.HK"
    assert mod._normalize_target_symbol("msft", "us") == "MSFT"


def test_sector_map_from_cache_converts_cn_keys(monkeypatch):
    monkeypatch.setattr(
        mod,
        "load_sector_cache",
        lambda: {
            "600519.SS": {
                "market": "cn",
                "sector": "Consumer Defensive",
            },
            "0700.HK": {
                "market": "hk",
                "sector": "Communication Services",
            },
            "MSFT": {
                "market": "us",
                "sector": "Technology",
            },
        },
    )
    out = mod.sector_map_from_cache()
    assert out["600519"] == "Consumer Defensive"
    assert out["0700.HK"] == "Communication Services"
    assert out["MSFT"] == "Technology"
