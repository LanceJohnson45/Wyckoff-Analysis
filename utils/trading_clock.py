from __future__ import annotations

import os
from bisect import bisect_right
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")
US_TZ = ZoneInfo("America/New_York")
try:
    DAY_SWITCH_HOUR = int(os.getenv("MARKET_DATA_READY_HOUR", "16"))
except Exception:
    DAY_SWITCH_HOUR = 16
try:
    US_DAY_SWITCH_HOUR = int(os.getenv("US_MARKET_DATA_READY_HOUR", "16"))
except Exception:
    US_DAY_SWITCH_HOUR = 16


def is_a_share_trading_day(d: date | None = None) -> bool:
    """判断给定日期是否为 A 股交易日（基于 akshare 交易日历）。"""
    if d is None:
        d = datetime.now(CN_TZ).date()
    if d.weekday() >= 5:
        return False
    try:
        from integrations.fetch_a_share_csv import _trade_dates_cached

        return d in set(_trade_dates_cached())
    except Exception:
        return d.weekday() < 5


def next_trading_day(after: date | None = None) -> date | None:
    """返回 after 之后最近的交易日，找不到则返回 None。"""
    base = after or datetime.now(CN_TZ).date()
    try:
        from integrations.fetch_a_share_csv import _trade_dates_cached

        dates = _trade_dates_cached()
        idx = bisect_right(dates, base)
        return dates[idx] if idx < len(dates) else None
    except Exception:
        for i in range(1, 8):
            d = base + timedelta(days=i)
            if d.weekday() < 5:
                return d
        return None


def resolve_end_calendar_day(
    now: datetime | None = None,
    switch_hour: int = DAY_SWITCH_HOUR,
) -> date:
    """
    日线目标日统一口径（北京时间）：
    - switch_hour(默认16):00 - 23:59 -> T（当天）
    - 00:00 - switch_hour(默认16):59 -> T-1（上一自然日）
    """
    dt = now.astimezone(CN_TZ) if now else datetime.now(CN_TZ)
    if dt.hour >= int(switch_hour):
        return dt.date()
    return (dt - timedelta(days=1)).date()


def resolve_end_calendar_day_for_market(
    market: str,
    now: datetime | None = None,
) -> date:
    market_norm = str(market or "cn").strip().lower()
    if market_norm == "us":
        dt = now.astimezone(US_TZ) if now else datetime.now(US_TZ)
        if dt.hour >= int(US_DAY_SWITCH_HOUR):
            return dt.date()
        return (dt - timedelta(days=1)).date()
    return resolve_end_calendar_day(now=now, switch_hour=DAY_SWITCH_HOUR)
