# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

from integrations import data_source


def _index_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-04-14", "2026-04-15"],
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 1200],
            "pct_chg": [0.5, 0.9],
        }
    )


class TestCnIndexYfinanceMapping:
    def test_numeric_codes_map_to_expected_yfinance_symbols(self):
        assert data_source._cn_index_to_yfinance_symbol("000001") == "000001.SS"
        assert data_source._cn_index_to_yfinance_symbol("000300") == "000300.SS"
        assert data_source._cn_index_to_yfinance_symbol("399001") == "399001.SZ"
        assert data_source._cn_index_to_yfinance_symbol("399006") == "399006.SZ"

    def test_sh_suffix_is_normalized_to_ss(self):
        assert data_source._cn_index_to_yfinance_symbol("000001.SH") == "000001.SS"
        assert data_source._cn_index_to_yfinance_symbol("000001.SS") == "000001.SS"


class TestFetchCnIndexHist:
    def test_cn_index_prefers_yfinance(self, monkeypatch):
        captured: dict[str, str] = {}

        def fake_yf(symbol: str, start: str, end: str) -> pd.DataFrame:
            captured["symbol"] = symbol
            captured["start"] = start
            captured["end"] = end
            return _index_frame()

        monkeypatch.setattr(data_source, "_fetch_index_yfinance", fake_yf)
        monkeypatch.setattr(
            data_source,
            "_fetch_index_akshare",
            lambda code, start, end: (_ for _ in ()).throw(AssertionError("akshare should not be called")),
        )

        result = data_source.fetch_index_hist("399006", "2026-03-01", "2026-04-15", market="cn")

        assert not result.empty
        assert captured == {
            "symbol": "399006.SZ",
            "start": "20260301",
            "end": "20260415",
        }

    def test_cn_index_falls_back_to_akshare_when_yfinance_history_is_too_short(
        self, monkeypatch
    ):
        monkeypatch.setattr(
            data_source,
            "_fetch_index_yfinance",
            lambda symbol, start, end: pd.DataFrame(
                {
                    "date": ["2026-04-15"],
                    "open": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "close": [1.0],
                    "volume": [1],
                    "pct_chg": [0.0],
                }
            ),
        )
        monkeypatch.setattr(
            data_source,
            "_fetch_index_akshare",
            lambda code, start, end: _index_frame(),
        )

        result = data_source.fetch_index_hist(
            "399006", "2026-03-01", "2026-04-15", market="cn"
        )

        assert len(result) == 2

    def test_cn_index_falls_back_to_akshare_when_yfinance_fails(self, monkeypatch):
        monkeypatch.setattr(
            data_source,
            "_fetch_index_yfinance",
            lambda symbol, start, end: (_ for _ in ()).throw(RuntimeError("yf fail")),
        )
        monkeypatch.setattr(
            data_source,
            "_fetch_index_akshare",
            lambda code, start, end: _index_frame(),
        )

        result = data_source.fetch_index_hist("000001", "2026-03-01", "2026-04-15", market="cn")

        assert list(result.columns) == ["date", "open", "high", "low", "close", "volume", "pct_chg"]
        assert len(result) == 2
