"""异步监督评估进程，取消/超时必须等候所属进程组真实退出。"""

import asyncio
import sys
from collections.abc import Awaitable, Callable
from tempfile import gettempdir
from typing import Literal, TypeVar

from stock_research.errors import ResearchUnavailable
from stock_research.evaluation import EvaluationReport
from stock_research.identity import digest
from stock_research.models import FrozenModel
from stock_research.processes import isolated_environment, stop_process_group
from stock_research.schemas.evaluation_execution import (
    MAX_EVALUATION_REPORT,
    MAX_EVALUATION_REQUEST,
    MAX_EVALUATION_RESPONSE,
    EvaluationFiles,
    EvaluationFrame,
)

Checkpoint = Callable[[EvaluationFrame], Awaitable[None]]
WorkerResult = TypeVar("WorkerResult")


class EvaluationExecutionError(ResearchUnavailable):
    """不透传子进程异常或路径，仅公开固定失败族。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__("历史评估未完成：" + code)


async def evaluation_response(
    process: asyncio.subprocess.Process,
    request: EvaluationFiles,
    checkpoint: Checkpoint | None,
) -> EvaluationFrame:
    """帧身份、输入、顺序和总字节数均校验，成功不能依靠文件存在推断。"""
    size, count, fingerprint = 0, 0, None
    started = frozen = False
    result = None
    while raw := await process.stdout.readline():
        size += len(raw)
        if size > MAX_EVALUATION_RESPONSE or result is not None:
            raise EvaluationExecutionError("evaluation_output_invalid")
        frame = EvaluationFrame.model_validate_json(raw)
        if frame.pid != process.pid or frame.source_hash != request.source_hash:
            raise EvaluationExecutionError("evaluation_source_changed")
        if frame.status == "started":
            if started:
                raise EvaluationExecutionError("evaluation_output_invalid")
            started = True
        elif not started:
            raise EvaluationExecutionError("evaluation_output_invalid")
        elif frame.status == "frozen":
            if frozen:
                raise EvaluationExecutionError("evaluation_output_invalid")
            frozen, fingerprint = True, frame.input_hash
        elif frame.status in {"section", "completed"}:
            if not frozen or frame.input_hash != fingerprint:
                raise EvaluationExecutionError("evaluation_output_invalid")
            if frame.status == "section":
                if frame.completed_sections != count + 1:
                    raise EvaluationExecutionError("evaluation_output_invalid")
                count = frame.completed_sections
            elif count != 20:
                raise EvaluationExecutionError("evaluation_output_invalid")
        if frame.status in {"completed", "failed"}:
            result = frame
        if checkpoint:
            await checkpoint(frame)
    await process.wait()
    if result is None or process.returncode != 0 and result.status == "completed":
        raise EvaluationExecutionError("evaluation_process_failed")
    if result.status == "failed":
        raise EvaluationExecutionError(result.error_code)
    if not result.report_path.resolve().is_relative_to(request.artifacts_path.resolve()):
        raise EvaluationExecutionError("evaluation_output_invalid")
    return result


async def execute_evaluation(
    request: EvaluationFiles,
    *,
    timeout: float,
    checkpoint: Checkpoint | None = None,
) -> EvaluationReport:
    """保留控制管道，父进程死亡和取消均触发子进程实际清理。"""

    async def consume(process: asyncio.subprocess.Process) -> EvaluationReport:
        result = await evaluation_response(process, request, checkpoint)
        return await asyncio.to_thread(read_evaluation_report, result, request)

    return await execute_worker(request, "stock_research.evaluation_worker", timeout=timeout, consume=consume)


async def execute_worker(
    request: FrozenModel,
    module: Literal["stock_research.evaluation_worker", "stock_research.selection_observation_worker"],
    *,
    timeout: float,
    consume: Callable[[asyncio.subprocess.Process], Awaitable[WorkerResult]],
) -> WorkerResult:
    """两类评估共用有界进程所有权，具体Adapter验证各自帧和产物。"""
    raw = (request.model_dump_json() + "\n").encode("utf-8")
    if len(raw) > MAX_EVALUATION_REQUEST:
        raise EvaluationExecutionError("evaluation_input_invalid")
    process = None
    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                module,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                env=isolated_environment(),
                cwd=gettempdir(),
                start_new_session=True,
            )
            process.stdin.write(raw)
            await process.stdin.drain()
            return await consume(process)
    except TimeoutError:
        raise EvaluationExecutionError("evaluation_timeout") from None
    except (OSError, ValueError):
        raise EvaluationExecutionError("evaluation_output_invalid") from None
    finally:
        if process:
            cleanup = asyncio.create_task(stop_process_group(process))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise


def read_evaluation_report(frame: EvaluationFrame, request: EvaluationFiles) -> EvaluationReport:
    """结果大小及协议绑定必须在发布PG前独立重读验证。"""
    if frame.report_path.stat().st_size > MAX_EVALUATION_REPORT:
        raise EvaluationExecutionError("evaluation_output_invalid")
    report = EvaluationReport.model_validate_json(frame.report_path.read_text(encoding="utf-8"))
    if (
        report.content_hash != frame.report_hash
        or report.input_hash != frame.input_hash
        or report.source_hash != request.source_hash
    ):
        raise EvaluationExecutionError("evaluation_output_invalid")
    if (
        report.cutoff != request.cutoff
        or report.allow_current_universe != request.allow_current_universe
        or report.parameter_hashes
        != {
            "martin": digest(request.martin.model_dump(mode="json")),
            "wyckoff": digest(request.wyckoff.model_dump(mode="json")),
        }
    ):
        raise EvaluationExecutionError("evaluation_output_invalid")
    return report
