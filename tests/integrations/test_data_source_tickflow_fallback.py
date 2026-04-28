# -*- coding: utf-8 -*-
"""data_source 中 A 股数据源降级链路测试。"""
from __future__ import annotations

import pandas as pd
import pytest

import integrations.data_source as ds


def _sample_cn_hist() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "日期": "2026-04-18",
                "开盘": 10.0,
                "最高": 10.5,
                "最低": 9.9,
                "收盘": 10.3,
                "成交量": 1000000.0,
                "成交额": 10000000.0,
                "涨跌幅": 1.2,
                "换手率": pd.NA,
                "振幅": 2.3,
            }
        ]
    )


def _disable_other_fallbacks(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATA_SOURCE_DISABLE_AKSHARE", "1")
    monkeypatch.setenv("DATA_SOURCE_DISABLE_BAOSTOCK", "1")
    monkeypatch.setenv("DATA_SOURCE_DISABLE_EFINANCE", "1")
    monkeypatch.delenv("DATA_SOURCE_DISABLE_TICKFLOW", raising=False)
    monkeypatch.delenv("DATA_SOURCE_DISABLE_YFINANCE", raising=False)


def test_cn_stock_to_yfinance_symbol_maps_exchange_suffix() -> None:
    assert ds._cn_stock_to_yfinance_symbol("600519") == "600519.SS"
    assert ds._cn_stock_to_yfinance_symbol("000001") == "000001.SZ"
    assert ds._cn_stock_to_yfinance_symbol("300750.SZ") == "300750.SZ"
    assert ds._cn_stock_to_yfinance_symbol("600519.SH") == "600519.SS"


def test_fetch_stock_hist_cn_prefers_yfinance_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.setenv("TICKFLOW_API_KEY", "dummy")
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)
    monkeypatch.setattr("integrations.tushare_client.get_pro", lambda: object())

    def _raise_lower_source_if_called(*args, **kwargs):
        raise RuntimeError("should_not_call")

    seen: dict[str, str] = {}

    def _fake_yfinance(symbol: str, *args, **kwargs) -> pd.DataFrame:
        seen["symbol"] = symbol
        return _sample_cn_hist()

    monkeypatch.setattr(ds, "_fetch_stock_yfinance", _fake_yfinance)
    monkeypatch.setattr(ds, "_fetch_stock_tickflow", _raise_lower_source_if_called)
    monkeypatch.setattr(ds, "_fetch_stock_tushare", _raise_lower_source_if_called)

    out = ds.fetch_stock_hist("600519", "2026-04-10", "2026-04-18", adjust="qfq")
    assert not out.empty
    assert out.attrs.get("source") == "yfinance"
    assert seen["symbol"] == "600519.SS"
    assert out.iloc[0]["日期"] == "2026-04-18"


def test_fetch_stock_hist_falls_back_to_tushare_when_tickflow_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.setenv("TICKFLOW_API_KEY", "dummy")
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)
    monkeypatch.setattr("integrations.tushare_client.get_pro", lambda: object())

    def _raise_tickflow(*args, **kwargs):
        raise RuntimeError("tickflow timeout")

    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("yf failed")),
    )
    monkeypatch.setattr(ds, "_fetch_stock_tickflow", _raise_tickflow)
    monkeypatch.setattr(ds, "_fetch_stock_tushare", lambda *args, **kwargs: _sample_cn_hist())

    out = ds.fetch_stock_hist("000001", "2026-04-10", "2026-04-18", adjust="qfq")
    assert not out.empty
    assert out.attrs.get("source") == "tushare"


def test_fetch_stock_hist_error_message_contains_tickflow_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)
    monkeypatch.setattr("integrations.tushare_client.get_pro", lambda: None)
    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("yf failed")),
    )

    with pytest.raises(RuntimeError) as exc:
        ds.fetch_stock_hist("000001", "2026-04-10", "2026-04-18", adjust="qfq")
    assert "yfinance→tickflow→tushare→akshare→baostock→efinance" in str(exc.value)


def test_fetch_stock_hist_us_prefers_yfinance_then_tickflow(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIKFLOW_API_KEY", "dummy")
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)

    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: _sample_cn_hist(),
    )

    def _raise_tickflow_if_called(*args, **kwargs):
        raise RuntimeError("should_not_call")

    monkeypatch.setattr(ds, "_fetch_stock_tickflow_global", _raise_tickflow_if_called)
    out = ds.fetch_stock_hist("AAPL", "2026-04-10", "2026-04-18", adjust="qfq", market="us")
    assert not out.empty
    assert out.attrs.get("source") == "yfinance"


def test_fetch_stock_hist_hk_falls_back_to_tickflow_when_yfinance_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIKFLOW_API_KEY", "dummy")
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("yf failed")),
    )
    monkeypatch.setattr(
        ds,
        "_fetch_stock_tickflow_global",
        lambda *args, **kwargs: _sample_cn_hist(),
    )
    out = ds.fetch_stock_hist("0700.HK", "2026-04-10", "2026-04-18", adjust="qfq", market="hk")
    assert not out.empty
    assert out.attrs.get("source") == "tickflow"
