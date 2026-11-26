"""正式初选的独立协议Adapter，共用批量任务进程所有权。"""

import asyncio
from collections.abc import Awaitable, Callable

from stock_research.evaluation_execution import EvaluationExecutionError, execute_worker
from stock_research.schemas.evaluation_execution import MAX_EVALUATION_RESPONSE
from stock_research.schemas.selection_observation_execution import SelectionObservationFiles, SelectionObservationFrame
from stock_research.schemas.selection_observations import SelectionObservationReport
from stock_research.selection_observation_cli import MAX_OBSERVATION_FILE_BYTES

ObservationCheckpoint = Callable[[SelectionObservationFrame], Awaitable[None]]


async def observation_response(
    process: asyncio.subprocess.Process, request: SelectionObservationFiles, checkpoint: ObservationCheckpoint
) -> SelectionObservationFrame:
    """只接受started→frozen→completed，身份和输入摘要贯穿所有帧。"""
    size, fingerprint = 0, None
    started = False
    result = None
    while raw := await process.stdout.readline():
        size += len(raw)
        if size > MAX_EVALUATION_RESPONSE or result is not None:
            raise EvaluationExecutionError("evaluation_output_invalid")
        frame = SelectionObservationFrame.model_validate_json(raw)
        if frame.pid != process.pid or frame.source_hash != request.source_hash:
            raise EvaluationExecutionError("evaluation_source_changed")
        if frame.status == "started":
            if started:
                raise EvaluationExecutionError("evaluation_output_invalid")
            started = True
        elif frame.status == "frozen":
            if not started or fingerprint is not None:
                raise EvaluationExecutionError("evaluation_output_invalid")
            fingerprint = frame.input_hash
            if request.expected_input_hash not in {None, fingerprint}:
                raise EvaluationExecutionError("evaluation_output_invalid")
        elif frame.status == "completed" and (fingerprint is None or frame.input_hash != fingerprint):
            raise EvaluationExecutionError("evaluation_output_invalid")
        if frame.status in {"completed", "failed"}:
            result = frame
        await checkpoint(frame)
    await process.wait()
    if result is None or process.returncode != 0 and result.status == "completed":
        raise EvaluationExecutionError("evaluation_process_failed")
    if result.status == "failed":
        raise EvaluationExecutionError(result.error_code)
    if not result.report_path.resolve().is_relative_to(request.artifacts_path.resolve()):
        raise EvaluationExecutionError("evaluation_output_invalid")
    return result


def read_observation_report(
    frame: SelectionObservationFrame, request: SelectionObservationFiles
) -> SelectionObservationReport:
    """结果发布前核对文件大小、原初选范围和完整摘要，不仅相信worker终帧。"""
    if frame.report_path.is_symlink() or frame.report_path.stat().st_size > MAX_OBSERVATION_FILE_BYTES:
        raise EvaluationExecutionError("evaluation_output_invalid")
    report = SelectionObservationReport.model_validate_json(frame.report_path.read_text(encoding="utf-8"))
    if (
        report.content_hash != frame.report_hash
        or report.input_hash != frame.input_hash
        or report.scope != request.scope
        or report.cutoff != request.cutoff
        or report.source_hash != request.source_hash
        or request.expected_input_hash not in {None, report.input_hash}
    ):
        raise EvaluationExecutionError("evaluation_output_invalid")
    return report


async def execute_selection_observation(
    request: SelectionObservationFiles, *, timeout: float, checkpoint: ObservationCheckpoint
) -> SelectionObservationReport:
    """完整协议/产物校验留在本Adapter，取消和进程清理沿共享实现。"""

    async def consume(process: asyncio.subprocess.Process) -> SelectionObservationReport:
        frame = await observation_response(process, request, checkpoint)
        return await asyncio.to_thread(read_observation_report, frame, request)

    return await execute_worker(
        request, "stock_research.selection_observation_worker", timeout=timeout, consume=consume
    )
