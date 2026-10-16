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
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.delenv("TIKFLOW_API_KEY", raising=False)
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

    def _raise_lower_source_if_called(*args, **kwargs):
        raise RuntimeError("should_not_call")

    seen: dict[str, str] = {}

    def _fake_yfinance(symbol: str, *args, **kwargs) -> pd.DataFrame:
        seen["symbol"] = symbol
        return _sample_cn_hist()

    monkeypatch.setattr(ds, "_fetch_stock_yfinance", _fake_yfinance)
    monkeypatch.setattr(ds, "_fetch_stock_tickflow", _raise_lower_source_if_called)

    out = ds.fetch_stock_hist("600519", "2026-04-10", "2026-04-18", adjust="qfq")
    assert not out.empty
    assert out.attrs.get("source") == "yfinance"
    assert seen["symbol"] == "600519.SS"
    assert out.iloc[0]["日期"] == "2026-04-18"


def test_fetch_stock_hist_falls_back_to_akshare_when_tickflow_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.delenv("DATA_SOURCE_DISABLE_AKSHARE", raising=False)
    monkeypatch.setenv("TICKFLOW_API_KEY", "dummy")
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)

    def _raise_tickflow(*args, **kwargs):
        raise RuntimeError("tickflow timeout")

    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("yf failed")),
    )
    monkeypatch.setattr(ds, "_fetch_stock_tickflow", _raise_tickflow)
    monkeypatch.setattr(ds, "_fetch_stock_akshare", lambda *args, **kwargs: _sample_cn_hist())

    out = ds.fetch_stock_hist("000001", "2026-04-10", "2026-04-18", adjust="qfq")
    assert not out.empty
    assert out.attrs.get("source") == "akshare"


def test_fetch_stock_hist_error_message_contains_tickflow_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)
    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("yf failed")),
    )

    with pytest.raises(RuntimeError) as exc:
        ds.fetch_stock_hist("000001", "2026-04-10", "2026-04-18", adjust="qfq")
    assert "yfinance→tickflow→akshare→baostock→efinance" in str(exc.value)


def test_fetch_stock_hist_network_failure_message_does_not_blame_delisting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.delenv("DATA_SOURCE_DISABLE_BAOSTOCK", raising=False)
    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT", None)
    monkeypatch.setattr(ds, "_TICKFLOW_CLIENT_READY", False)
    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("yfinance empty after date filter")
        ),
    )
    monkeypatch.setattr(
        ds,
        "_fetch_stock_baostock",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("baostock: 网络接收错误。")
        ),
    )

    with pytest.raises(RuntimeError) as exc:
        ds.fetch_stock_hist("000001", "2026-04-10", "2026-04-18", adjust="qfq")

    message = str(exc.value)
    assert "诊断提示：疑似上游行情源瞬时断连" in message
    assert "退市或处于长期停牌期" not in message


def test_fetch_stock_baostock_suppresses_vendor_timeout_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class _Result:
        error_code = "10002007"
        error_msg = "网络接收错误。"
        fields = []

        def next(self) -> bool:
            print("timed out")
            print("接收数据异常，请稍后再试。")
            return False

    class _Baostock:
        def query_history_k_data_plus(self, *args, **kwargs):
            print("timed out")
            print("接收数据异常，请稍后再试。")
            return _Result()

    monkeypatch.setattr(ds, "_DATA_SOURCE_DEBUG", False)
    monkeypatch.setattr(ds, "_ensure_baostock_login", lambda: _Baostock())

    with pytest.raises(RuntimeError):
        ds._fetch_stock_baostock("000001", "20260410", "20260418")

    captured = capsys.readouterr()
    assert "timed out" not in captured.out
    assert "接收数据异常" not in captured.out


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


@pytest.mark.parametrize("market,symbol", [("cn", "001237"), ("us", "NEWIPO")], ids=["cn-before-ipo", "us-before-ipo"])
def test_normal_explicit_prelisting_boundary_survives_fallback_chain(monkeypatch: pytest.MonkeyPatch, market: str, symbol: str) -> None:
    """【正常值】确认上市前没有K线是有效边界，不再当多源故障或退市。"""
    from datetime import date
    from integrations.market_data_policy import BeforeListingRange

    _disable_other_fallbacks(monkeypatch)
    monkeypatch.setattr(ds, "_fetch_stock_yfinance", lambda *args: (_ for _ in ()).throw(BeforeListingRange(date(2026, 5, 22))))

    with pytest.raises(BeforeListingRange) as exc:
        ds.fetch_stock_hist(symbol, "2026-01-01", "2026-05-21", market=market)

    assert exc.value.listing_date == date(2026, 5, 22)


def test_normal_yahoo_metadata_confirms_filter_empty_before_ipo(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】请求扩展区间含首日，但目标区间在上市前时用元数据确认边界。"""
    from datetime import date
    import yfinance as yf
    from integrations.market_data_policy import BeforeListingRange

    frame = pd.DataFrame({"Open": [10], "High": [11], "Low": [9], "Close": [10], "Volume": [1000]}, index=pd.DatetimeIndex(["2026-05-22"], name="Date"))
    monkeypatch.setattr(yf, "download", lambda *args, **kwargs: frame)
    monkeypatch.setattr(ds, "yahoo_first_trade_date", lambda symbol: date(2026, 5, 22))

    with pytest.raises(BeforeListingRange) as exc:
        ds._fetch_stock_yfinance("001237.SZ", "20250101", "20260521")

    assert exc.value.listing_date == date(2026, 5, 22)


def test_error_yahoo_empty_without_metadata_is_not_confirmed_delisting(monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】空下载且无首次交易日，仍为待审核故障而非自动批准排除。"""
    import yfinance as yf
    from integrations.market_data_policy import BeforeListingRange

    monkeypatch.setattr(yf, "download", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(ds, "yahoo_first_trade_date", lambda symbol: None)

    with pytest.raises(RuntimeError, match="yfinance empty") as exc:
        ds._fetch_stock_yfinance("000004.SZ", "20260901", "20261015")

    assert not isinstance(exc.value, BeforeListingRange)


def test_boundary_baostock_empty_does_not_open_network_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
    """【边界值】连续计数到9次后合法空行情不能将其他股票备用源熔断。"""
    _disable_other_fallbacks(monkeypatch)
    monkeypatch.delenv("DATA_SOURCE_DISABLE_BAOSTOCK", raising=False)
    monkeypatch.setenv("DATA_SOURCE_DISABLE_YFINANCE", "1")
    monkeypatch.setattr(ds, "_BAOSTOCK_CONSEC_FAILS", 9)
    monkeypatch.setattr(ds, "_BAOSTOCK_CIRCUIT_OPEN", False)
    monkeypatch.setattr(ds, "_BAOSTOCK_CIRCUIT_THRESHOLD", 10)
    monkeypatch.setattr(ds, "_fetch_stock_baostock", lambda *args: (_ for _ in ()).throw(RuntimeError("baostock empty")))

    with pytest.raises(RuntimeError):
        ds.fetch_stock_hist("001237", "2025-01-01", "2026-05-21")

    assert ds._BAOSTOCK_CONSEC_FAILS == 0
    assert not ds._BAOSTOCK_CIRCUIT_OPEN
