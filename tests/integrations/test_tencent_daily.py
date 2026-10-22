"""腾讯备用源的价格口径、单位和真实降级边界回归。"""
# lint-redline:ignore PS-07 单文件扫描误判；实际同目录另有test_data_source_tickflow_fallback.py。

from datetime import date
import json
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from typing import NotRequired, TypedDict

import pandas as pd
import pytest

from integrations import data_source as ds
from integrations import tencent_daily as tx


class Feed(TypedDict):
    """网络样本及请求记录，供离线边界验证使用。"""

    symbol: str
    rows: list[list[object]]
    factors: list[dict[str, object]]
    urls: list[str]
    fail_host: NotRequired[str]


@pytest.fixture
def feed(monkeypatch: pytest.MonkeyPatch) -> Feed:
    """仅替换网络响应，解析、比例复权和降级逻辑使用生产代码。"""
    state = {"symbol": "sh600519", "rows": [["2024-01-02", "100", "110", "120", "90", "12"]],
             "factors": [{"d": "1900-01-01", "f": "2"}], "urls": []}

    def read(url: str) -> str:
        state["urls"].append(url)
        if "sina.com.cn" in url:
            return 'var factor=' + json.dumps({"data": state["factors"]}) + ';/* trailing */'
        if state.get("fail_host") and state["fail_host"] in url:
            raise URLError("upstream unavailable")
        return json.dumps({"code": 0, "data": {state["symbol"]: {"day": state["rows"]}}})

    monkeypatch.setattr(tx, "_read_text", read)
    return state


@pytest.mark.parametrize("symbol,expected", [("600519", 1200), ("000001", 1200), ("300750", 1200), ("688981", 12), ("689009", 12)])
def test_volume_is_shares_and_missing_amount_stays_unknown(feed: Feed, symbol: str, expected: int) -> None:
    """科创板保留股，其余乘100；缺失成交额不伪造。"""
    feed["symbol"] = ("sh" if symbol.startswith("6") else "sz") + symbol
    result = tx.fetch_tencent_daily(symbol, "20240102", "20240102", "")
    assert result.iloc[0]["成交量"] == expected
    assert pd.isna(result.iloc[0]["成交额"])
    assert result.iloc[0]["收盘"] == 110
    assert len(feed["urls"]) == 1


@pytest.mark.parametrize("adjust,expected", [("qfq", 55), ("hfq", 220)])
def test_ratio_adjustment_uses_sina_direction(feed: Feed, adjust: str, expected: int) -> None:
    """验证复权方向，不把腾讯原生等差复权当成比例复权。"""
    result = tx.fetch_tencent_daily("600519", "20240102", "20240102", adjust)
    assert result.iloc[0]["收盘"] == expected
    assert result.iloc[0]["成交量"] == 1200
    assert parse_qs(urlparse(feed["urls"][0]).query)["param"][0].endswith(",640,")


def test_factors_apply_at_effective_date(feed: Feed) -> None:
    """除权日当天使用新因子，成交量保持原始股数。"""
    feed["rows"].append(["2024-01-03", "50", "55", "60", "45", "24"])
    feed["factors"].append({"d": "2024-01-03", "f": "1"})
    result = tx.fetch_tencent_daily("600519", "20240102", "20240103", "qfq")
    assert result["收盘"].tolist() == [55, 55]
    assert result.iloc[1]["涨跌幅"] == 0
    assert result["成交量"].tolist() == [1200, 2400]


@pytest.mark.parametrize("factors", [[], [{"d":"2024-02-01","f":1}], [{"d":"1900-01-01","f":0}], [{"d":"1900-01-01","f":"nan"}]])
def test_invalid_factor_must_not_return_unadjusted_prices(feed: Feed, factors: list) -> None:
    """空因子或因子覆盖不足必须失败。"""
    feed["factors"] = factors
    with pytest.raises((RuntimeError, ValueError)):
        tx.fetch_tencent_daily("600519", "20240102", "20240102", "qfq")


@pytest.mark.parametrize("bad_row", [
    ["2024-01-03",100,110,120,90,12],
    ["2024-01-02",100,110,105,90,12],
    ["2024-01-02",100,110,120,90,-1],
    ["2024-01-02","nan",110,120,90,12],
    ["2024-01-02",True,110,120,90,12],
])
def test_invalid_or_out_of_range_rows_are_rejected(feed: Feed, bad_row: list) -> None:
    """坏价、越界日期和负成交量不能进入缓存。"""
    feed["rows"] = [bad_row]
    with pytest.raises((RuntimeError, ValueError)):
        tx.fetch_tencent_daily("600519", "20240102", "20240102", "")


def test_duplicate_dates_are_rejected(feed: Feed) -> None:
    """重复日期不能冒充完整交易日覆盖。"""
    feed["rows"] *= 2
    with pytest.raises(RuntimeError, match="duplicate"):
        tx.fetch_tencent_daily("600519", "20240102", "20240102", "")


def test_host_failure_tries_next_entry(feed: Feed) -> None:
    """一个腾讯入口失效后，下一入口仍可提供日线。"""
    feed["fail_host"] = tx._TENCENT_HOSTS[0]
    result = tx.fetch_tencent_daily("600519", "20240102", "20240102", "")
    assert result.iloc[0]["收盘"] == 110
    assert len(feed["urls"]) == 2


def test_long_windows_do_not_silently_truncate(monkeypatch: pytest.MonkeyPatch) -> None:
    """超过单页窗口时逐段请求，保留各段数据。"""
    windows = []

    def window(symbol: str, start: str, end: str) -> list:
        windows.append((start,end))
        return [[start,100,110,120,90,12]]

    monkeypatch.setattr(tx,"_raw_window",window)
    result=tx.fetch_tencent_daily("600519","20200101","20241231","")
    assert len(result)==len(windows)==3
    assert windows[0][0]=="2020-01-01" and windows[-1][1]=="2024-12-31"
    assert all((date.fromisoformat(b)-date.fromisoformat(a)).days < 640 for a,b in windows)


def test_daily_update_falls_back_to_tencent_and_preserves_missing_date_checks(feed: Feed, monkeypatch: pytest.MonkeyPatch) -> None:
    """通过生产入口验证腾讯接线，过时结果仍转到东财。"""
    monkeypatch.setenv("DATA_SOURCE_DISABLE_YFINANCE","1")
    monkeypatch.setenv("DATA_SOURCE_DISABLE_TICKFLOW","1")
    monkeypatch.setenv("DATA_SOURCE_DISABLE_TENCENT","0")
    monkeypatch.setenv("DATA_SOURCE_DISABLE_AKSHARE","0")
    result=ds.fetch_stock_hist("600519","20240102","20240102",required_dates=[date(2024,1,2)])
    assert result.attrs["source"]=="tencent"
    assert result.iloc[0]["收盘"]==55
    complete=result.copy()
    complete["日期"]="2024-01-03"
    monkeypatch.setattr(ds,"_fetch_stock_akshare",lambda *args:complete)
    result=ds.fetch_stock_hist("600519","20240102","20240103",required_dates=[date(2024,1,3)])
    assert result.attrs["source"]=="akshare"
    assert result.iloc[0]["日期"]=="2024-01-03"


@pytest.mark.parametrize("symbol", ["920002","430047","00700","AAPL"])
def test_unsupported_symbols_rejected_without_network(feed: Feed, symbol: str) -> None:
    """避免把北交所、美港代码误路由至沪深。"""
    with pytest.raises(ValueError):
        tx.fetch_tencent_daily(symbol,"20240102","20240102","")
    assert feed["urls"]==[]
