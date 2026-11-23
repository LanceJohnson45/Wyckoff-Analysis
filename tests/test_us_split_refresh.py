"""拆股及无密钥备用源的回归验证。"""

from datetime import date

import pandas as pd
import pytest

from scripts import funnel_prewarm as prewarm
from integrations.fetch_a_share_csv import TradingWindow


def test_split_refreshes_adjusted_history_before_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """复现 SNDK 拆股当天仅补一行会留下八倍旧价格的问题。"""
    days = [date(2026, 11, 19), date(2026, 11, 20)]
    window = TradingWindow(start_trade_date=days[0], end_trade_date=days[-1])
    calls = []
    writes = []

    def fetch(**kwargs):
        calls.append(kwargs)
        frame = pd.DataFrame({"日期": [str(d) for d in (days if len(calls) > 1 else days[-1:])],
                              "开盘": 90., "最高": 100., "最低": 70., "收盘": 80., "成交量": 1000.})
        frame.attrs.update(source="yfinance", split_dates=["2026-11-20"])
        return frame

    monkeypatch.setattr(prewarm, "fetch_stock_hist", fetch)
    monkeypatch.setattr(prewarm, "upsert_cache_data", lambda **kw: writes.append(kw["df"]) or True)
    monkeypatch.setattr(prewarm, "load_cached_dates", lambda *a, **kw: days)
    result = prewarm._prefetch_one("SNDK", "us", 2, trading_window=window, expected_dates=days,
                                  initial_cached_dates={days[0]}, end_calendar_day=days[-1])
    assert result[1] == "gap_repaired"
    assert len(calls) == 2
    assert calls[1]["start"] < days[0]
    assert writes[0]["date"].tolist() == [str(d) for d in days]


def test_missing_key_still_uses_free_tickflow(monkeypatch: pytest.MonkeyPatch) -> None:
    """Yahoo 临时空值且无付费密钥时，免费批量接口仍须接通。"""
    from integrations import data_source as ds, us_tickflow_free

    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.delenv("TIKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(ds, "_fetch_stock_yfinance", lambda *a: pd.DataFrame())
    frame = pd.DataFrame({"日期": ["2026-11-20"], "开盘": [90.], "最高": [100.], "最低": [70.], "收盘": [80.], "成交量": [1000.]})
    monkeypatch.setattr(us_tickflow_free, "fetch_us_tickflow_free", lambda *a: frame)
    day = date(2026, 11, 20)
    result = ds.fetch_stock_hist("SNDK", day, day, market="us", required_dates=[day])
    assert result.attrs["source"] == "tickflow_free"


def test_free_fallback_cannot_mark_old_data_as_current(monkeypatch: pytest.MonkeyPatch) -> None:
    """免费源只有昨日时仍须报错，不把缺数隐藏为成功。"""
    from integrations import data_source as ds, us_tickflow_free

    monkeypatch.delenv("TICKFLOW_API_KEY", raising=False)
    monkeypatch.delenv("TIKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(ds, "_fetch_stock_yfinance", lambda *a: pd.DataFrame())
    frame = pd.DataFrame({"日期": ["2026-11-19"], "开盘": [90.], "最高": [100.], "最低": [70.], "收盘": [80.], "成交量": [1000.]})
    monkeypatch.setattr(us_tickflow_free, "fetch_us_tickflow_free", lambda *a: frame)
    day = date(2026, 11, 20)
    with pytest.raises(RuntimeError, match="US stock fetch failed"):
        ds.fetch_stock_hist("SNDK", day, day, market="us", required_dates=[day])


def test_yahoo_transient_empty_is_retried_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """一次空返回应优先重试 Yahoo，无需请求备用源。"""
    from integrations import data_source as ds

    frame = pd.DataFrame({"日期": ["2026-11-20"], "开盘": [90.], "最高": [100.], "最低": [70.], "收盘": [80.], "成交量": [1000.]})
    replies = iter([pd.DataFrame(), frame])
    monkeypatch.setattr(ds, "_fetch_stock_yfinance", lambda *a: next(replies))
    day = date(2026, 11, 20)
    assert ds.fetch_stock_hist("SNDK", day, day, market="us", required_dates=[day]).attrs["source"] == "yfinance"
