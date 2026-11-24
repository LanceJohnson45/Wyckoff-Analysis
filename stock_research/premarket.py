"""固定workflow的无副作用计划入口；按原计划纽约日期计算，不启动研究。"""

import argparse
import asyncio
import json
import os
from datetime import datetime, time
from pathlib import Path

from stock_research.artifacts import write_once
from stock_research.calendar import exchange_calendar, session_times
from stock_research.identity import canonical_json, now
from stock_research.schemas.premarket import NEW_YORK, PremarketInput

WORKFLOW = "stock-research-premarket"
MAX_INPUT_BYTES = 16_384


def plan_premarket(data: PremarketInput, observed_at: datetime) -> dict[str, object]:
    """自动补跑不越过原日08:50；显式手动迟到仅输出计划与迟到标记。"""
    if observed_at.tzinfo is None:
        raise ValueError("计划观察时间必须包含时区")
    schedule = data.schedule
    target = schedule.target_session
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
    deadline = schedule.execution_deadline
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
    """plan不加载身份；execute只读取独立workflow身份，模型/PG留在API。"""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--identity-file", type=Path, help="operator固定的专用workflow配置路径；仅execute")
    parser.add_argument("--at", type=datetime.fromisoformat, help="仅用于无副作用计划验证的带时区观察时间")
    args = parser.parse_args(argv)
    if args.plan and args.identity_file is not None:
        parser.error("--identity-file只允许--execute")
    if args.execute and args.at is not None:
        parser.error("--at只允许无副作用的--plan验证")
    try:
        source, output = workflow_paths(dict(os.environ))
        with source.open("rb") as handle:
            raw = handle.read(MAX_INPUT_BYTES + 1)
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError("workflow输入超过限额")
        data = PremarketInput.model_validate_json(raw)
        status = 0
        if args.plan:
            result = plan_premarket(data, args.at or now())
        else:
            from stock_research.premarket_workflow import execute_workflow
            from stock_research.workflow_settings import load_workflow_identity

            outcome = asyncio.run(execute_workflow(data, load_workflow_identity(args.identity_file)))
            result = outcome.model_dump(mode="json")
            status = 1 if outcome.error_code else 0
        write_once(output, canonical_json(result) + "\n")
    except (OSError, ValueError, KeyError, RecursionError):
        print(json.dumps({"mode": "execute" if args.execute else "plan", "error": "盘前输入、专用身份或文件边界无效"}))
        return 1
    print(canonical_json(result))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
