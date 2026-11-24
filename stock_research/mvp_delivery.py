"""单用户盘前发布：冻结已知事实，通过Personal原有持久发送账本交付。"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from stock_research.artifacts import write_once
from stock_research.identity import canonical_json
from stock_research.mvp_settings import MvpSettings
from stock_research.processes import isolated_environment, stop_process_group
from stock_research.schemas.premarket import PremarketInput, PremarketReceipt

WORKER_MAX_BYTES = 100_000


class DeliveryReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: Literal["sent", "silent", "ready", "retry", "unknown", "busy", "exhausted", "unconfirmed"]
    message_ref: str = ""


class Publication(BaseModel):
    model_config = ConfigDict(extra="forbid")
    body: str
    completed_stages: int
    report_id: str | None


def deliveries_sent(result: dict[str, object]) -> bool:
    """有恢复发布时必须也取得sent，否则后续tick仍需沿原业务键重放。"""
    recovery = result.get("recovery")
    return result.get("initial", {}).get("state") == "sent" and (recovery is None or recovery.get("state") == "sent")


async def call_worker(
    settings: MvpSettings, kind: Literal["refresh", "notify", "status"], payload: dict[str, object], timeout: float
) -> dict[str, object]:
    """独立进程只读取自己的配置；超时/取消回收本次进程组。"""
    root = settings.source_root if kind == "refresh" else settings.personal_root
    env_file = settings.source_env_file if kind == "refresh" else settings.personal_env_file
    script = "stock_research_delivery_status.py" if kind == "status" else "stock_research_mvp_worker.py"
    worker = Path(__file__).resolve().parents[1] / "scripts" / script
    if kind == "status":
        root = worker.parents[1]
    environment = isolated_environment()
    environment["PYTHONPATH"] = str(root)
    if kind == "notify":
        environment["HOME"] = str(Path.home())
    process = await asyncio.create_subprocess_exec(
        sys.executable if kind == "status" else str(root / ".venv/bin/python"),
        str(worker),
        *([] if kind == "status" else [kind]),
        "--root",
        str(root),
        "--env-file",
        str(env_file),
        cwd=root,
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        start_new_session=True,
        limit=WORKER_MAX_BYTES + 1,
    )
    try:
        async with asyncio.timeout(timeout):
            raw = (canonical_json(payload) + "\n").encode()
            if len(raw) > WORKER_MAX_BYTES:
                raise ValueError("MVP输入超过上限")
            process.stdin.write(raw)
            await process.stdin.drain()
            response = await process.stdout.readline()
            await asyncio.wait_for(process.wait(), 5)
            if kind == "status" and process.returncode:
                raise ValueError("通知状态进程未完成")
            result = json.loads(response)
            if not isinstance(result, dict) or len(response) > WORKER_MAX_BYTES:
                raise ValueError("MVP回执无效")
            return result
    finally:
        await stop_process_group(process)


def publication(data: PremarketInput, receipt: PremarketReceipt | None, phase: str, error: str | None) -> Publication:
    """只展示回执已确认的名单，缺项明确保留状态。"""
    run = receipt.report.run if receipt and receipt.report else None
    lines = [f"美股盘前研究 · {data.schedule.target_session} · {'恢复更新' if phase == 'recovery' else '首报'}"]
    if data.schedule.trigger_kind == "manual":
        lines.append("手动执行；不代表按时发布。")
    lines.append(f"状态：{receipt.status if receipt else 'preparing'}" + (f"；{error}" if error else ""))
    count = 0
    report_id = None
    if run:
        report_id = str(receipt.report.report_id)
        count = sum(stage.status == "completed" for stage in run.stages)
        lines += [
            f"行情截止：{run.as_of}；覆盖：{run.quality.aligned_count}/{run.quality.universe_count}",
            f"报告：{report_id}（版本{receipt.report.revision}）；完成阶段：{count}/{len(run.stages)}",
        ]
        for strategy, label in (("martin", "Martin"), ("wyckoff", "Wyckoff")):
            selection = next((item for item in run.selections if item.strategy == strategy), None)
            lines.append(f"\n{label}")
            if selection is None:
                lines.append("初选尚未完成；不据此推断没有机会。")
                continue
            if not selection.decision.items:
                lines.append("空名单：" + selection.decision.empty_reason.text)
            for item in selection.decision.items:
                lines += [
                    f"• {item.symbol}：{item.rationale.text}",
                    "  风险：" + "；".join(point.text for point in item.risks),
                    "  待确认：" + "；".join(point.text for point in item.confirmation_conditions),
                ]
            review = next(
                (item for item in run.reviews if item.decision.target_selection_id == str(selection.id)), None
            )
            lines += (
                [f"  交叉评议 {item.symbol}：{item.stance}；{item.rationale.text}" for item in review.decision.items]
                if review
                else ["交叉评议尚未完成。"]
            )
    else:
        lines.append("尚无可发布的固定研究报告；数据准备或模型仍未完成，不作为买卖依据。")
    lines.append("研究观察名单；交易仍须满足确认条件与风控。")
    body = "\n".join(lines)
    if len(body) > 28_000:
        body = body[:27_900] + "\n内容较长，详情请按上述报告编号查询。"
    return Publication(body=body, completed_stages=count, report_id=report_id)


def freeze_publication(directory: Path, candidate: Publication) -> Publication:
    path = directory / "publication.json"
    if path.exists():
        return Publication.model_validate_json(path.read_bytes())
    write_once(path, candidate.model_dump_json() + "\n")
    path.chmod(0o600)
    return candidate


async def publish(
    settings: MvpSettings, data: PremarketInput, phase: Literal["initial", "recovery"], candidate: Publication
) -> tuple[Publication, DeliveryReceipt]:
    directory = settings.state_path / str(data.schedule.target_session) / phase
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    frozen = await asyncio.to_thread(freeze_publication, directory, candidate)
    payload = {
        "workspace": data.schedule.workspace_id,
        "window": f"us:{data.schedule.target_session}:{phase}",
        "body": frozen.body,
        "directory": str(directory),
    }
    try:
        response = await call_worker(settings, "notify", payload, 125)
        result = DeliveryReceipt.model_validate(response)
    except (OSError, ValueError, TimeoutError):
        result = DeliveryReceipt(state="unconfirmed")
    return frozen, result
