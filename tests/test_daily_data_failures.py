"""日线任务指数降级与北交所错误路由的回归。"""

from datetime import date

import akshare as ak
import pandas as pd
import pytest

from integrations import data_source as ds


def _frame() -> pd.DataFrame:
    days = pd.bdate_range("2026-04-01", "2026-10-16")
    return pd.DataFrame({"date": days.date, "open": 10., "high": 11., "low": 9., "close": 10., "volume": 1000})


def test_error_index_primary_sources_fail_but_sina_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】Yahoo和东财失败时，真实降级链仍返回同一个沪深300。"""
    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Connection reset by peer")

    monkeypatch.setattr(ds, "_fetch_index_yfinance", fail)
    monkeypatch.setattr(ds, "_fetch_index_akshare", fail)
    calls = []

    def sina(symbol: str) -> pd.DataFrame:
        calls.append(symbol)
        return _frame()

    monkeypatch.setattr(ak, "stock_zh_index_daily", sina)
    frame = ds.fetch_index_hist("000300", "20260401", "20261016", market="cn")
    assert calls == ["sh000300"]
    assert frame.iloc[-1]["date"] == "2026-10-16"
    assert "pct_chg" in frame.columns


def test_error_bse_does_not_enter_serial_baostock_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】北交所空数据应审核，不得伪装深圳股票排队查询BaoStock。"""
    for key in ("TICKFLOW_API_KEY", "TIKFLOW_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    for key in ("YFINANCE", "TICKFLOW", "AKSHARE", "EFINANCE"):
        monkeypatch.setenv(f"DATA_SOURCE_DISABLE_{key}", "1")
    monkeypatch.delenv("DATA_SOURCE_DISABLE_BAOSTOCK", raising=False)
    monkeypatch.setattr(ds, "_baostock_circuit_state", lambda: (False, ""))
    calls = []
    monkeypatch.setattr(ds, "_fetch_stock_baostock", lambda *args: calls.append(args))
    with pytest.raises(RuntimeError, match="unsupported_bse"):
        ds.fetch_stock_hist("920002", date(2026, 10, 9), date(2026, 10, 16))
    assert calls == []


def test_normal_bse_uses_sina_with_correct_market_and_units(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】全池新增的北交所走bj代码，成交量保留股、换手率转百分比。"""
    for key in ("TICKFLOW_API_KEY", "TIKFLOW_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    for key in ("AKSHARE", "YFINANCE", "TICKFLOW"):
        monkeypatch.delenv(f"DATA_SOURCE_DISABLE_{key}", raising=False)
    seen = []

    def daily(**kwargs: object) -> pd.DataFrame:
        seen.append(kwargs)
        f = _frame().tail(2).copy()
        f["turnover"] = .02
        return f

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("有效北交所数据不应落入后续备用源")

    monkeypatch.setattr(ak, "stock_zh_a_daily", daily)
    monkeypatch.setattr(ds, "_fetch_stock_baostock", unexpected)
    monkeypatch.setattr(ds, "_fetch_stock_akshare", unexpected)
    result = ds.fetch_stock_hist("920002", "20261015", "20261016", market="cn")
    assert seen == [{"symbol": "bj920002", "start_date": "20261015", "end_date": "20261016", "adjust": "qfq"}]
    assert result.attrs["source"] == "sina_bse"
    assert result.iloc[-1]["成交量"] == 1000
    assert result.iloc[-1]["换手率"] == 2


@pytest.mark.parametrize("bad_primary", ["short", "stale"])
def test_error_benchmark_rejects_short_or_stale_primary_and_falls_back(monkeypatch: pytest.MonkeyPatch, bad_primary: str) -> None:
    """【异常值】有数据不等于成功；过短或昨日数据仍须切源到目标日。"""
    valid = _frame()
    bad = valid.tail(2) if bad_primary == "short" else valid.iloc[:-1]
    monkeypatch.setattr(ds, "_fetch_index_yfinance", lambda *args: bad)
    monkeypatch.setattr(ds, "_fetch_index_akshare", lambda *args: bad)
    monkeypatch.setattr(ak, "stock_zh_index_daily", lambda **kwargs: valid)

    result = ds.fetch_index_hist("000300", "20260401", "20261016", market="cn", min_rows=100, required_end=date(2026, 10, 16))

    assert len(result) > 100
    assert result.iloc[-1]["date"] == "2026-10-16"


def test_error_all_index_sources_fail_reports_each_cause(monkeypatch: pytest.MonkeyPatch) -> None:
    """【异常值】全线异常仍报错，携带逐源原因便于人工审核。"""
    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Connection reset")

    monkeypatch.setattr(ds, "_fetch_index_yfinance", fail)
    monkeypatch.setattr(ds, "_fetch_index_akshare", fail)
    monkeypatch.setattr(ak, "stock_zh_index_daily", fail)
    with pytest.raises(RuntimeError, match="yfinance=.*eastmoney=.*sina="):
        ds.fetch_index_hist("000300", "20260401", "20261016", market="cn")


def test_normal_refresh_benchmark_writes_sina_same_index_after_primary_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】从Action脚本入口到降级归一化及写入边界验证，隔离云端写操作。"""
    from integrations.fetch_a_share_csv import TradingWindow
    from scripts import refresh_market_benchmarks as benchmark

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Connection reset")

    monkeypatch.setattr(ds, "_fetch_index_yfinance", fail)
    monkeypatch.setattr(ds, "_fetch_index_akshare", fail)
    monkeypatch.setattr(ak, "stock_zh_index_daily", lambda **kwargs: _frame())
    monkeypatch.setattr(benchmark, "resolve_end_calendar_day_for_market", lambda market: date(2026, 10, 17))
    monkeypatch.setattr(benchmark, "_resolve_trading_window", lambda *args: TradingWindow(date(2026, 10, 16), date(2026, 10, 16)))
    writes = []

    def write(symbol: str, adjust: str, source: str, frame: pd.DataFrame, **kwargs: object) -> bool:
        writes.append((symbol, frame))
        return True

    monkeypatch.setattr(benchmark, "upsert_cache_data", write)
    rows = benchmark.refresh_benchmark("cn")
    assert rows == len(_frame())
    assert writes[0][0] == "INDEX:CN:000300"
    assert writes[0][1].iloc[-1]["date"] == "2026-10-16"
