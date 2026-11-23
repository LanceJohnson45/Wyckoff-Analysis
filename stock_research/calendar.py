"""XNYS 正式交易日历和盘前期限，交易日与操作时间分别处理。"""

from datetime import date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from stock_research.identity import SHANGHAI, now


@lru_cache(maxsize=1)
def exchange_calendar() -> xcals.ExchangeCalendar:
    """加载固定依赖版本提供的纽约证券交易所日历。"""
    return xcals.get_calendar("XNYS", start="1990-01-01", end="2040-12-31")


def sessions_ending(as_of: date, count: int) -> list[date]:
    """取得截止日前 count 个正式交易日，拒绝非交易日截止日期。"""
    calendar = exchange_calendar()
    stamp = pd.Timestamp(as_of)
    if not calendar.is_session(stamp):
        raise ValueError(f"{as_of} 不是 XNYS 交易日")
    end = calendar.sessions.get_loc(stamp)
    if end < count - 1:
        raise ValueError("日历历史长度不足")
    return [value.date() for value in calendar.sessions[end - count + 1 : end + 1]]


def session_times(target: date) -> tuple[datetime, datetime, date]:
    """返回开盘、报告期限和前一完整交易日，自动处理夏冬令时。"""
    calendar = exchange_calendar()
    if not calendar.is_session(pd.Timestamp(target)):
        raise ValueError(f"{target} 不是 XNYS 交易日")
    opening = calendar.session_open(pd.Timestamp(target)).to_pydatetime().astimezone(SHANGHAI)
    previous = calendar.previous_session(pd.Timestamp(target)).date()
    return opening, opening - timedelta(minutes=60), previous


def latest_complete_session(at: datetime | None = None) -> date:
    """按正式收盘判断最近完整日线，包含半日市和纽约跨日。"""
    at = at or now()
    if at.tzinfo is None:
        raise ValueError("判断完整交易日必须传入带时区时间")
    calendar = exchange_calendar()
    day = pd.Timestamp(at.astimezone(ZoneInfo("America/New_York")).date())
    session = calendar.date_to_session(day, direction="previous")
    if calendar.session_close(session) > pd.Timestamp(at):
        session = calendar.previous_session(session)
    return session.date()
