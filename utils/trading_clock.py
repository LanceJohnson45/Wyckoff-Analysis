# -*- coding: utf-8 -*-
from __future__ import annotations

import os
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
