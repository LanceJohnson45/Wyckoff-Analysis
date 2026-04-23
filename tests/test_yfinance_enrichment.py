from __future__ import annotations

import pandas as pd

from integrations import yfinance_enrichment as mod


def test_normalize_yfinance_symbol_by_market():
    assert mod.normalize_yfinance_symbol("000001", "cn") == "000001.SZ"
    assert mod.normalize_yfinance_symbol("600519", "cn") == "600519.SS"
    assert mod.normalize_yfinance_symbol("9988", "hk") == "9988.HK"
    assert mod.normalize_yfinance_symbol("msft", "us") == "MSFT"


def test_build_market_cap_map_from_cached_shares(tmp_path, monkeypatch):
    cache_path = tmp_path / "shares.json"
    monkeypatch.setattr(mod, "_SHARES_CACHE_PATH", cache_path)
    monkeypatch.setattr(mod, "_SHARES_REFRESH_MAX_PER_RUN", 0)
    mod._save_shares_items(
        {
            "MSFT": {
                "symbol": "MSFT",
                "market": "us",
                "shares_outstanding": 10_000_000,
                "currency": "USD",
                "updated_ts": mod._now_ts(),
            }
        }
    )

    df = pd.DataFrame({"close": [100.0, 120.0]})
    out, stats = mod.build_market_cap_map_from_shares(
        symbols=["MSFT"],
        market="us",
        df_map={"MSFT": df},
        base_map={},
        refresh_missing=True,
    )

    assert round(out["MSFT"], 2) == 86.4
    assert stats["computed"] == 1
    assert stats["refreshed"] == 0


def test_enrichment_context_formats_factor_and_news():
    text = mod._format_enrichment_context(
        {
            "factors": {
                "estimate_revision_score": 2.5,
                "earnings_surprise_score": 1.0,
                "target_price_gap_pct": 15.2,
                "earnings_event_risk": {"has_event": True, "days_to_event": 5},
            },
            "news": [
                {
                    "pubDate": "2026-04-23T00:00:00Z",
                    "title": "Company raises guidance",
                }
            ],
        }
    )

    assert "预期上修=+2.5" in text
    assert "目标价偏离=+15.2%" in text
    assert "未来5天" in text
    assert "Company raises guidance" in text
