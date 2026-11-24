"""有界的固定任务调用与回查；模型/数据库身份始终留在研究API。"""

import asyncio
import signal
from typing import Literal
from uuid import UUID

from stock_research.errors import RETRYABLE_MODEL_ERRORS
from stock_research.identity import now
from stock_research.schemas.api import ApiModel
from stock_research.schemas.premarket import PremarketInput, PremarketReceipt
from stock_research.workflow_client import PremarketClient, WorkflowError
from stock_research.workflow_settings import WorkflowSettings

PREPARATION_DELAYS = (60, 180)
RETRYABLE_CODES = RETRYABLE_MODEL_ERRORS | {"deadline_exceeded", "quant_timeout", "quant_process_failed"}


class WorkflowOutcome(ApiModel):
    mode: Literal["execute"] = "execute"
    receipt: PremarketReceipt | None
    error_code: str | None
    delivery: Literal["not_requested"] = "not_requested"


def recoverable(receipt: PremarketReceipt) -> bool:
    """只恢复明确临时失败，拒绝用重试掩盖模型结构/证据或配置失败。"""
    if (
        receipt.run is None
        or receipt.run.error_code == "run_deadline_exceeded"
        or (receipt.origin is not None and now() >= receipt.origin.execution_deadline)
    ):
        return False
    codes = [stage.error for stage in receipt.run.stages if stage.status == "failed"]
    codes += [job.error_code for job in receipt.run.quant if job.status == "failed"]
    return bool(codes) and all(code in RETRYABLE_CODES for code in codes)


async def stop_job(client: PremarketClient, receipt: PremarketReceipt | None) -> PremarketReceipt | None:
    """中止只操作已确认的原任务；未知受理结果不通过新POST猜测或创建。"""
    if receipt is None or receipt.status not in {"pending", "running", "unconfirmed", "failed_preparation"}:
        return receipt
    async with asyncio.timeout(25):
        try:
            result = await client.call("POST", job_id=receipt.job_id, action="cancel")
        except WorkflowError as error:
            if error.code != "request_conflict":
                raise
            result = await read_confirmed(client, receipt.job_id)
        if result.status not in {"completed", "partial", "failed", "cancelled", "skipped", "expired"}:
            raise WorkflowError("cancel_unconfirmed")
        return result


async def read_confirmed(client: PremarketClient, job_id: UUID) -> PremarketReceipt:
    """观察失败只重读原任务，不能据此推断结束或重启执行。"""
    while True:
        try:
            return await client.call("GET", job_id=job_id)
        except WorkflowError as error:
            if error.code not in {"transport_unknown", "request_unavailable", "request_conflict"}:
                raise
            await asyncio.sleep(2)


async def follow_job(client: PremarketClient, progress: list[PremarketReceipt]) -> PremarketReceipt:
    """原事件重放安全重试准备，成功初选和固定报告均留在原run。"""
    preparation_retries = stage_retries = 0
    receipt = progress[-1]
    while True:
        if receipt.status in {"failed_preparation", "unconfirmed"}:
            if preparation_retries >= len(PREPARATION_DELAYS):
                return receipt
            delay = PREPARATION_DELAYS[preparation_retries]
            preparation_retries += 1
            action = None
        elif receipt.status in {"failed", "partial"} and recoverable(receipt):
            delay = 60 if stage_retries == 0 else 180
            stage_retries += 1
            action = "retry"
        elif receipt.status in {"pending", "running"}:
            await asyncio.sleep(2)
            receipt = await read_confirmed(client, receipt.job_id)
            progress.append(receipt)
            continue
        else:
            return receipt
        deadline = client.data.schedule.execution_deadline
        if receipt.origin is not None:
            deadline = min(deadline, receipt.origin.execution_deadline)
        if (deadline - now()).total_seconds() <= delay:
            return receipt
        await asyncio.sleep(delay)
        try:
            receipt = await client.call("POST", job_id=receipt.job_id if action else None, action=action)
        except WorkflowError as error:
            if error.code not in {"transport_unknown", "request_unavailable", "request_conflict"}:
                raise
            receipt = await read_confirmed(client, receipt.job_id)
        progress.append(receipt)


async def run_workflow(
    data: PremarketInput,
    settings: WorkflowSettings,
    *,
    client: PremarketClient | None = None,
    progress: list[PremarketReceipt] | None = None,
) -> WorkflowOutcome:
    """整体按原事件截止；拥有者取消或超时后有界收尾，不发送通知。"""
    client = client or PremarketClient(settings, data)
    progress = progress if progress is not None else []
    error = None
    try:
        remaining = (data.schedule.execution_deadline - now()).total_seconds()
        async with asyncio.timeout(min(3000, remaining) if remaining > 0 else 5):
            for attempt in range(3):
                try:
                    progress.append(await client.call("POST"))
                    break
                except WorkflowError as failure:
                    if (
                        failure.code not in {"transport_unknown", "request_unavailable", "request_conflict"}
                        or attempt == 2
                    ):
                        raise
                    await asyncio.sleep(PREPARATION_DELAYS[attempt])
            receipt = await follow_job(client, progress)
            return WorkflowOutcome(
                receipt=receipt, error_code="preparation_unconfirmed" if receipt.status == "unconfirmed" else None
            )
    except asyncio.CancelledError:
        error = "workflow_cancelled"
    except TimeoutError:
        error = "workflow_deadline"
    except WorkflowError as failure:
        error = failure.code
    receipt = progress[-1] if progress else None
    try:
        receipt = await stop_job(client, receipt)
    except (TimeoutError, WorkflowError):
        error = "cancel_unconfirmed" if receipt else "admission_unconfirmed"
    if receipt is None and error not in {"request_denied", "workflow_action_invalid"}:
        error = "admission_unconfirmed"
    return WorkflowOutcome(receipt=receipt, error_code=error)


async def execute_workflow(data: PremarketInput, settings: WorkflowSettings) -> WorkflowOutcome:
    """仅独立CLI进程接管SIGTERM，库调用不改变宿主的信号处理。"""
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        return await run_workflow(data, settings)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
