"""美股 Yahoo 缺数时使用 TickFlow 免费批量日线接口。"""

import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from math import isfinite

import httpx
import pandas as pd

from integrations.tickflow_client import parse_ohlcv_payload


async def _download(symbol: str, start: str, end: str) -> pd.DataFrame:
    first = datetime.strptime(start, "%Y%m%d").date()
    last = datetime.strptime(end, "%Y%m%d").date()
    tz = ZoneInfo("America/New_York")
    code = f"{symbol.upper()}.US"
    params = {
        "symbols": code, "period": "1d", "adjust": "forward",
        "count": min((last - first).days + 2, 1000),
        "start_time": int(datetime.combine(first, time.min, tzinfo=tz).timestamp() * 1000),
        "end_time": int(datetime.combine(last + timedelta(days=1), time.min, tzinfo=tz).timestamp() * 1000) - 1,
    }
    async with httpx.AsyncClient(timeout=15, follow_redirects=False) as client:
        for attempt in range(2):
            try:
                response = await client.get("https://free-api.tickflow.org/v1/klines/batch", params=params)
                response.raise_for_status()
                payload = response.json()
                break
            except (httpx.TransportError, httpx.HTTPStatusError):
                if attempt:
                    raise
                await asyncio.sleep(2)
    frame = parse_ohlcv_payload({"data": payload.get("data", {}).get(code)})
    if frame.empty:
        raise RuntimeError("TickFlow 免费日线为空")
    frame = frame.loc[frame["date"].between(str(first), str(last))].copy()
    values = frame[["open", "high", "low", "close", "volume"]]
    valid = values.map(isfinite).all(axis=1) & values.gt(0).all(axis=1)
    valid &= frame["low"].le(frame[["open", "close"]].min(axis=1))
    valid &= frame["high"].ge(frame[["open", "close"]].max(axis=1))
    frame = frame.loc[valid].copy()
    if frame.empty:
        raise RuntimeError("TickFlow 免费日线无有效 OHLCV")
    return frame.rename(columns={"date": "日期", "open": "开盘", "high": "最高", "low": "最低", "close": "收盘", "volume": "成交量", "amount": "成交额"})


def fetch_us_tickflow_free(symbol: str, start: str, end: str) -> pd.DataFrame:
    """获取指定区间的有效前复权日线，日期缺口由调用方核验。"""
    return asyncio.run(_download(symbol, start, end))
