"""单用户MVP固定任务：自动补拉、定点冻结首报及Personal飞书持久交付。"""

import argparse
import asyncio
import json
import os
import signal
from pathlib import Path

from stock_research.artifacts import data_lock, write_once
from stock_research.identity import canonical_json, now
from stock_research.models import UniverseVersion
from stock_research.mvp_delivery import call_worker, publication, publish
from stock_research.mvp_settings import MvpSettings, load_mvp_settings
from stock_research.premarket import MAX_INPUT_BYTES, plan_premarket, workflow_paths
from stock_research.premarket_workflow import WorkflowOutcome, run_workflow
from stock_research.schemas.premarket import PremarketInput, PremarketPlan, PremarketReceipt
from stock_research.workflow_settings import load_workflow_identity


async def research_pipeline(
    settings: MvpSettings, data: PremarketInput, plan: PremarketPlan, progress: list[PremarketReceipt]
) -> tuple[dict[str, object], WorkflowOutcome]:
    """补拉在API准备之前，来源失败仍让正式质量门禁如实裁决缓存。"""
    remaining = (data.schedule.execution_deadline - now()).total_seconds()
    try:
        universe = UniverseVersion.model_validate_json(await asyncio.to_thread(settings.universe_path.read_bytes))
        symbols = list(universe.active(plan.as_of))
        refresh = await call_worker(
            settings, "refresh", {"as_of": str(plan.as_of), "symbols": symbols}, min(900, max(1, remaining - 300))
        )
    except (OSError, ValueError, TimeoutError):
        refresh = {"status": "unconfirmed", "error_code": "source_refresh_unconfirmed"}
    try:
        outcome = await run_workflow(data, load_workflow_identity(settings.workflow_identity_file), progress=progress)
    except (OSError, ValueError):
        outcome = WorkflowOutcome(
            receipt=progress[-1] if progress else None, error_code="workflow_configuration_invalid"
        )
    return refresh, outcome


async def run_mvp(settings: MvpSettings, data: PremarketInput) -> dict[str, object]:
    plan = PremarketPlan.model_validate(plan_premarket(data, now()))
    if plan.status in {"skipped_non_session", "before_start", "expired"}:
        return {"mode": "mvp", "status": plan.status, "delivery": "not_requested"}
    progress: list[PremarketReceipt] = []
    task = asyncio.create_task(research_pipeline(settings, data, plan, progress))
    try:
        if data.schedule.trigger_kind == "scheduled":
            await asyncio.sleep(max(0, (plan.report_due - now()).total_seconds()))
        else:
            await asyncio.wait({task})
        outcome = task.result()[1] if task.done() and not task.exception() else None
        receipt = outcome.receipt if outcome else progress[-1] if progress else None
        initial, delivery = await publish(
            settings, data, "initial", publication(data, receipt, "initial", outcome.error_code if outcome else None)
        )
        refresh, outcome = await task
        recovery = None
        latest = publication(data, outcome.receipt, "recovery", outcome.error_code)
        if latest.completed_stages > initial.completed_stages and now() < data.schedule.execution_deadline:
            _, recovery = await publish(settings, data, "recovery", latest)
        return {
            "mode": "mvp",
            "refresh": refresh,
            "research": outcome.model_dump(mode="json"),
            "initial": delivery.model_dump(),
            "recovery": recovery.model_dump() if recovery else None,
        }
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def execute_mvp(settings: MvpSettings, data: PremarketInput) -> dict[str, object]:
    loop, task = asyncio.get_running_loop(), asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        return await run_mvp(settings, data)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="operator固定的0600路径配置")
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        source, output = workflow_paths(dict(os.environ))
        raw = source.read_bytes()
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError("盘前输入过大")
        data = PremarketInput.model_validate_json(raw)
        settings = load_mvp_settings(args.config)
        settings.state_path.mkdir(parents=True, exist_ok=True, mode=0o700)
        settings.state_path.chmod(0o700)
        with data_lock(settings.state_path, "us", str(data.schedule.target_session)):
            result = asyncio.run(execute_mvp(settings, data))
        write_once(output, canonical_json(result) + "\n")
    except (OSError, ValueError, KeyError, TimeoutError):
        print(json.dumps({"mode": "mvp", "error_code": "mvp_execution_unconfirmed"}))
        return 1
    print(canonical_json({key: value for key, value in result.items() if key != "research"}))
    return int(result.get("initial", {}).get("state", "sent") != "sent")


if __name__ == "__main__":
    raise SystemExit(main())
