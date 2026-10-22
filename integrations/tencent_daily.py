"""腾讯沪深个股原始日线及新浪比例复权适配，不使用腾讯等差复权价格。"""
# lint-redline:ignore PS-07 单文件扫描误判；实际integrations目录包含data_source等多个现有模块。

from __future__ import annotations

from datetime import timedelta
import json
import math
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

# lint-redline:ignore PS-10 公开行情提供方固定入口，属于数据源协议，不是部署环境地址。
_TENCENT_HOSTS = ("https://web.ifzq.gtimg.cn", "https://proxy.finance.qq.com", "https://web.sqt.gtimg.cn")
_REQUEST_TIMEOUT = 8
_WINDOW_DAYS = 640  # 自然日窗口不会触及接口的640根日线截断上限。
_PRICE_COLUMNS = ["开盘", "最高", "最低", "收盘"]


def _read_text(url: str) -> str:
    """有界读取公开行情响应，保持 TLS 证书校验。"""
    # lint-redline:ignore PS-10 Referer是上游请求协议要求，与本项目部署地址无关。
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
    with urlopen(request, timeout=_REQUEST_TIMEOUT) as response:
        return response.read().decode("utf-8-sig")


def _raw_window(symbol: str, start: str, end: str) -> list:
    """同一区间依次尝试腾讯入口，拒绝异常或缺少原始日线字段的响应。"""
    errors = []
    query = urlencode({"param": f"{symbol},day,{start},{end},640,"})
    for host in _TENCENT_HOSTS:
        try:
            payload = json.loads(_read_text(f"{host}/appstock/app/fqkline/get?{query}"))
            if not isinstance(payload, dict) or payload.get("code") != 0:
                raise ValueError("tencent response code or schema invalid")
            rows = payload["data"][symbol]["day"]
            if not isinstance(rows, list):
                raise ValueError("tencent day is not a list")
            return rows
        except (URLError, TimeoutError, OSError, ValueError, KeyError, TypeError) as exc:
            errors.append(f"{host}: {type(exc).__name__}")
    raise RuntimeError(f"tencent raw daily unavailable: {'; '.join(errors)}")


def _number(value: object) -> float:
    """拒绝布尔、缺失或非有限数值，避免异常行情进入缓存。"""
    if isinstance(value, bool):
        raise ValueError("boolean market value")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite market value")
    return number


def fetch_tencent_daily(symbol: str, start: str, end: str, adjust: str) -> pd.DataFrame:
    """返回现有取数链使用的中文列日线；复权只作用于价格，成交量统一为股。

    Args:
        symbol: 沪深个股的六位证券代码。
        start: 起始日期，YYYYMMDD。
        end: 截止日期，YYYYMMDD，包含当日。
        adjust: 空字符串为原始价格，qfq/hfq 使用新浪比例因子。

    Returns:
        按日期升序的行情；成交额及换手率未知时保留缺失值。

    Raises:
        ValueError: 参数或行情数值无效。
        RuntimeError: 数据不可用、日期重复/越界或复权因子覆盖不全。
    """
    if len(symbol) != 6 or not symbol.isascii() or not symbol.isdigit() or symbol[0] not in "036":
        raise ValueError("tencent daily only supports mainland Shanghai/Shenzhen stock codes")
    if adjust not in ("", "qfq", "hfq"):
        raise ValueError("unsupported adjustment")
    first = pd.to_datetime(start, format="%Y%m%d").date()
    last = pd.to_datetime(end, format="%Y%m%d").date()
    if first > last:
        raise ValueError("start after end")
    ticker = ("sh" if symbol.startswith("6") else "sz") + symbol
    records, seen = [], set()
    cursor = first
    while cursor <= last:
        stop = min(cursor + timedelta(days=_WINDOW_DAYS - 1), last)
        for row in _raw_window(ticker, cursor.isoformat(), stop.isoformat()):
            day = pd.to_datetime(row[0], format="%Y-%m-%d").date()
            if not cursor <= day <= stop or day in seen:
                raise RuntimeError("tencent duplicate or out-of-window date")
            seen.add(day)
            opening, closing, high, low, volume = [_number(value) for value in row[1:6]]
            if min(opening, closing, high, low) <= 0 or low > min(opening, closing) or high < max(opening, closing) or volume < 0:
                raise RuntimeError("tencent invalid OHLCV")
            records.append({"日期": day.isoformat(), "开盘": opening, "收盘": closing, "最高": high, "最低": low,
                            "成交量": volume if symbol.startswith(("688", "689")) else volume * 100})
        cursor = stop + timedelta(days=1)
    if not records:
        raise RuntimeError("tencent empty")
    frame = pd.DataFrame(records).sort_values("日期").reset_index(drop=True)
    if adjust:
        # lint-redline:ignore PS-10 新浪公开复权文件的固定协议地址，非部署环境服务。
        text = _read_text(f"https://finance.sina.com.cn/realstock/company/{ticker}/{adjust}.js")
        brace = text.find("{")
        if brace < 0:
            raise RuntimeError("sina adjustment response missing JSON")
        payload, _ = json.JSONDecoder().raw_decode(text[brace:])
        factors = pd.DataFrame(payload.get("data", []))
        if factors.empty or not {"d", "f"}.issubset(factors.columns):
            raise RuntimeError("sina adjustment factors empty or invalid")
        factors = factors[["d", "f"]].rename(columns={"d": "日期", "f": "factor"})
        factors["日期"] = pd.to_datetime(factors["日期"], format="%Y-%m-%d")
        factors["factor"] = factors["factor"].map(_number)
        if factors["日期"].duplicated().any() or factors["factor"].le(0).any():
            raise RuntimeError("sina duplicate or invalid adjustment factor")
        frame["日期"] = pd.to_datetime(frame["日期"])
        frame = pd.merge_asof(frame, factors.sort_values("日期"), on="日期", direction="backward")
        if frame["factor"].isna().any():
            raise RuntimeError("sina adjustment factors do not cover history")
        for column in _PRICE_COLUMNS:
            frame[column] = frame[column] / frame["factor"] if adjust == "qfq" else frame[column] * frame["factor"]
            if not frame[column].map(math.isfinite).all() or frame[column].le(0).any():
                raise RuntimeError("invalid adjusted price")
        frame = frame.drop(columns="factor")
        frame["日期"] = frame["日期"].dt.strftime("%Y-%m-%d")
    previous = frame["收盘"].shift(1)
    frame["涨跌幅"] = frame["收盘"].pct_change(fill_method=None) * 100
    frame["振幅"] = (frame["最高"] - frame["最低"]) / previous * 100
    # 腾讯日线的基础六字段没有真实成交额，不以复权收盘价乘成交量伪造。
    frame["成交额"] = pd.NA
    frame["换手率"] = pd.NA
    return frame
