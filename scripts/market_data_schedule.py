"""在定时全量取数前，核对行情目标日及交易日历。"""
from __future__ import annotations

import argparse
from collections.abc import Callable, Sequence
from datetime import date, datetime
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import structlog

from utils.trading_clock import resolve_end_calendar_day_for_market


def schedule_decision(
    *, event: str, market: str, now: datetime,
    load_calendar: Callable[[], Sequence[date]],
) -> dict[str, str | bool]:
    """手动补数继续执行；定时任务只更新已收盘的交易日。"""
    target = resolve_end_calendar_day_for_market(market, now=now)
    result: dict[str, str | bool] = {
        "market": market, "trigger_event": event, "now": now.isoformat(),
        "target_date": target.isoformat(), "should_run": True,
        "reason": "manual_backfill",
    }
    if event != "schedule":
        return result
    if target.weekday() >= 5:
        return {**result, "should_run": False, "reason": "target_is_weekend"}
    dates = load_calendar()
    if not dates or max(dates) < target:
        raise RuntimeError(f"交易日历未覆盖目标日 {target}，无法判定是否开市")
    return {
        **result, "should_run": target in dates,
        "reason": "target_is_trading_day" if target in dates else "target_is_holiday",
    }


def _load_cn_calendar() -> Sequence[date]:
    from integrations.fetch_a_share_csv import _trade_dates_cached
    from scripts.prewarm_workers import bounded_call

    # 不允许工作日近似日历把法定节假日当作交易日。
    os.environ["ALLOW_APPROX_TRADE_CALENDAR"] = "0"
    return bounded_call(_trade_dates_cached, timeout=60)


def main() -> int:
    """记录调度决定，并通过 Actions output 传给后续取数步骤。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", choices=["cn"], required=True)
    args = parser.parse_args()
    result = schedule_decision(
        event=os.getenv("GITHUB_EVENT_NAME", "workflow_dispatch"), market=args.market,
        now=datetime.now(ZoneInfo("Asia/Shanghai")), load_calendar=_load_cn_calendar,
    )
    logger = structlog.wrap_logger(
        structlog.PrintLogger(), processors=[structlog.processors.JSONRenderer(ensure_ascii=False)],
    )
    logger.info("market_data_schedule", **result)
    if output := os.getenv("GITHUB_OUTPUT"):
        with Path(output).open("a", encoding="utf-8") as stream:
            stream.write(f"should_run={str(result['should_run']).lower()}\n")
            stream.write(f"target_date={result['target_date']}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
