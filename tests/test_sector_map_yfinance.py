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


def test_industry_map_from_cache_converts_cn_keys(monkeypatch):
    monkeypatch.setattr(
        mod,
        "load_sector_cache",
        lambda: {
            "600519.SS": {
                "market": "cn",
                "industry": "Beverages - Wineries & Distilleries",
            },
            "0700.HK": {
                "market": "hk",
                "industry": "Internet Content & Information",
            },
            "MSFT": {
                "market": "us",
                "industry": "Software - Infrastructure",
            },
        },
    )
    out = mod.industry_map_from_cache()
    assert out["600519"] == "Beverages - Wineries & Distilleries"
    assert out["0700.HK"] == "Internet Content & Information"
    assert out["MSFT"] == "Software - Infrastructure"


def test_market_cap_map_from_cache_by_market(monkeypatch):
    monkeypatch.setattr(
        mod,
        "load_sector_cache",
        lambda: {
            "600519.SS": {
                "market": "cn",
                "market_cap_yi": 21000.5,
            },
            "0700.HK": {
                "market": "hk",
                "market_cap_yi": 31500.0,
            },
            "MSFT": {
                "market": "us",
                "market_cap_yi": 220000.0,
            },
        },
    )
    cn = mod.market_cap_map_from_cache(market="cn")
    hk = mod.market_cap_map_from_cache(market="hk")
    us = mod.market_cap_map_from_cache(market="us")
    assert cn["600519"] == 21000.5
    assert hk["0700.HK"] == 31500.0
    assert us["MSFT"] == 220000.0
