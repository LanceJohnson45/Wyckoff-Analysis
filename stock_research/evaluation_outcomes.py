"""选股后表现的描述性观测，不模拟委托、持仓或交易执行。"""

from collections.abc import Iterable
from datetime import date
from typing import Literal

import pandas as pd
from pydantic import Field

from core.trade_friction import round_trip_cost_pct
from stock_research.calendar import exchange_calendar
from stock_research.models import Bar, FrozenModel, normalize_symbol

OBSERVATION_HORIZONS = (1, 3, 5, 10, 20)
LONGEST_HORIZON = max(OBSERVATION_HORIZONS)


class OutcomeWindow(FrozenModel):
    """百分数统一以百分点为单位；未成熟或缺数的窗口不填零收益。"""

    horizon: Literal[1, 3, 5, 10, 20]
    target_session: date
    status: Literal["completed", "pending", "missing_data"]
    missing_sessions: tuple[str, ...] = ()
    gross_return_pct: float | None = None
    cost_pct: float = Field(ge=0)
    net_return_pct: float | None = None
    benchmark_return_pct: float | None = None
    net_excess_pct: float | None = None
    mfe_pct: float | None = None
    mae_pct: float | None = None


def observation_sessions(as_of: date) -> tuple[date, ...]:
    """固定纽约正式交易日，不以股票实际存在的行数替代观察窗口。"""
    calendar = exchange_calendar()
    stamp = pd.Timestamp(as_of)
    if not calendar.is_session(stamp):
        raise ValueError("观测起点必须是XNYS交易日")
    start = calendar.sessions.get_loc(stamp)
    sessions = tuple(day.date() for day in calendar.sessions[start : start + LONGEST_HORIZON + 1])
    if len(sessions) != LONGEST_HORIZON + 1:
        raise ValueError("正式日历不足以覆盖观察窗口")
    return sessions


def price_index(bars: Iterable[Bar]) -> dict[tuple[str, str, date], Bar]:
    """一次索引供多个入选复用；相同股票和基准身份仍分开保存。"""
    result = {}
    for bar in bars:
        key = (bar.kind, bar.symbol, bar.session_date)
        if key in result:
            raise ValueError("观测行情有重复交易日")
        result[key] = bar
    return result


def indexed_outcomes(
    symbol: str,
    as_of: date,
    cutoff: date,
    prices: dict[tuple[str, str, date], Bar],
    *,
    reference: Literal["previous_close", "forward_open"] = "previous_close",
) -> tuple[OutcomeWindow, ...]:
    """收盘截面和前向开盘分别取参考价，缺数不缩小观测分母。"""
    symbol = normalize_symbol(symbol)
    if reference not in {"previous_close", "forward_open"}:
        raise ValueError("未知观测参考点")
    if as_of > cutoff and reference == "previous_close":
        raise ValueError("选股日期不能晚于评估截止日")
    sessions = observation_sessions(as_of)
    cost = round_trip_cost_pct(code=symbol + ".US")
    result = []
    for horizon in OBSERVATION_HORIZONS:
        end = horizon - 1 if reference == "forward_open" else horizon
        fields = dict(horizon=horizon, target_session=sessions[end], cost_pct=cost)
        if sessions[end] > cutoff:
            result.append(OutcomeWindow(**fields, status="pending"))
            continue
        required = [
            (kind, code, day) for kind, code in (("stock", symbol), ("benchmark", "SPY")) for day in sessions[: end + 1]
        ]
        missing = tuple(f"{kind}/{code}/{day}" for kind, code, day in required if (kind, code, day) not in prices)
        if missing:
            result.append(OutcomeWindow(**fields, status="missing_data", missing_sessions=missing))
            continue
        stock = [prices["stock", symbol, day] for day in sessions[: end + 1]]
        benchmark = [prices["benchmark", "SPY", day] for day in sessions[: end + 1]]
        base = stock[0].open if reference == "forward_open" else stock[0].close
        spy_base = benchmark[0].open if reference == "forward_open" else benchmark[0].close
        observed = stock if reference == "forward_open" else stock[1:]
        gross = (stock[-1].close / base - 1) * 100
        benchmark_return = (benchmark[-1].close / spy_base - 1) * 100
        result.append(
            OutcomeWindow(
                **fields,
                status="completed",
                gross_return_pct=gross,
                net_return_pct=gross - cost,
                benchmark_return_pct=benchmark_return,
                net_excess_pct=gross - cost - benchmark_return,
                mfe_pct=max(0.0, (max(bar.high for bar in observed) / base - 1) * 100),
                mae_pct=min(0.0, (min(bar.low for bar in observed) / base - 1) * 100),
            )
        )
    return tuple(result)
