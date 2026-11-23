"""固定workflow的无副作用计划入口；按原计划纽约日期计算，不启动研究。"""

import argparse
import json
import os
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, model_validator

from stock_research.artifacts import write_once
from stock_research.calendar import exchange_calendar, session_times
from stock_research.identity import canonical_json, now
from stock_research.schemas.api import ApiModel

NEW_YORK = ZoneInfo("America/New_York")
WORKFLOW = "stock-research-premarket"
MAX_INPUT_BYTES = 16_384


class ScheduleContext(ApiModel):
    """唯一股票计划及可核对的原事件身份，不接受命令、路径或凭据。"""

    schedule_id: Literal["stock-research-premarket"]
    event_id: str = Field(min_length=1, max_length=160)
    scheduled_for: AwareDatetime
    workspace_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    trigger_kind: Literal["scheduled", "manual"]

    @model_validator(mode="after")
    def coherent_event(self) -> "ScheduleContext":
        stamp = self.scheduled_for.astimezone(UTC).isoformat().replace("+00:00", "Z")
        prefix = "manual-" if self.trigger_kind == "manual" else ""
        if self.event_id != f"{self.schedule_id}:{prefix}{stamp}":
            raise ValueError("原计划时刻与事件身份不一致")
        local = self.scheduled_for.astimezone(NEW_YORK)
        if self.trigger_kind == "scheduled" and local.timetz().replace(tzinfo=None) != time(8):
            raise ValueError("自动盘前事件必须为纽约08:00的原计划时刻")
        return self


class PremarketInput(ApiModel):
    schedule: ScheduleContext


def plan_premarket(data: PremarketInput, observed_at: datetime) -> dict[str, object]:
    """自动补跑不越过原日08:50；显式手动迟到仅输出计划与迟到标记。"""
    if observed_at.tzinfo is None:
        raise ValueError("计划观察时间必须包含时区")
    schedule = data.schedule
    target = schedule.scheduled_for.astimezone(NEW_YORK).date()
    result = {
        "mode": "plan",
        "research_started": False,
        "schedule": schedule.model_dump(mode="json"),
        "target_session": target.isoformat(),
        "observed_at": observed_at.isoformat(),
        "as_of": None,
        "report_due": None,
        "recovery_until": None,
        "execution_deadline": None,
        "budget_seconds": 0,
        "late": False,
        "status": "skipped_non_session",
    }
    if not exchange_calendar().is_session(target.isoformat()):
        return result
    _opening, due, previous = session_times(target)
    start = datetime.combine(target, time(8), NEW_YORK)
    stop = datetime.combine(target, time(8, 50), NEW_YORK)
    late = observed_at > due
    deadline = schedule.scheduled_for + timedelta(seconds=3000) if schedule.trigger_kind == "manual" else stop
    status = "within_window"
    if observed_at < schedule.scheduled_for or observed_at < start:
        status = "before_start"
    elif observed_at >= deadline:
        status = "expired"
    elif observed_at >= stop:
        status = "late_manual"
    budget = max(0, int((deadline - observed_at).total_seconds())) if status != "before_start" else 0
    return {
        **result,
        "as_of": previous.isoformat(),
        "report_due": due.isoformat(),
        "recovery_until": stop.isoformat(),
        "execution_deadline": deadline.isoformat() if budget else None,
        "budget_seconds": budget,
        "late": late,
        "status": status,
    }


def workflow_paths(environment: dict[str, str]) -> tuple[Path, Path]:
    """只消费runner授予的同一run目录input/output，不接受业务入参里的路径。"""
    if environment.get("SA_WORKFLOW_NAME") != WORKFLOW:
        raise ValueError("只允许固定盘前workflow")
    root = Path(environment["SA_RUN_ARTIFACTS"]).resolve(strict=True)
    source, output = (Path(environment[key]) for key in ("SA_RUN_INPUT_JSON", "SA_RUN_OUTPUT_JSON"))
    for path, name in ((source, "input.json"), (output, "output.json")):
        if not path.is_absolute() or path.is_symlink() or path.name != name:
            raise ValueError("workflow文件身份不符")
        parent = path.parent.resolve(strict=True)
        if parent == root or not parent.is_relative_to(root):
            raise ValueError("workflow文件越界")
    if source.parent.resolve() != output.parent.resolve() or not source.is_file():
        raise ValueError("workflow文件不属于同一运行")
    return source, output


def main(argv: list[str] | None = None) -> int:
    """显式--plan运行不加载Settings，不提供任何HTTP、模型或通知执行。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", required=True)
    parser.add_argument("--at", type=datetime.fromisoformat, help="仅用于无副作用计划验证的带时区观察时间")
    args = parser.parse_args(argv)
    try:
        source, output = workflow_paths(dict(os.environ))
        with source.open("rb") as handle:
            raw = handle.read(MAX_INPUT_BYTES + 1)
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError("workflow输入超过限额")
        result = plan_premarket(PremarketInput.model_validate_json(raw), args.at or now())
        write_once(output, canonical_json(result) + "\n")
    except (OSError, ValueError, KeyError, RecursionError):
        print(json.dumps({"mode": "plan", "research_started": False, "error": "盘前计划输入或文件边界无效"}))
        return 1
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
