"""API/CLI量化进程监督：取消/超时必须等候本次子进程真实退出。"""

import asyncio
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import structlog

from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.errors import ResearchUnavailable
from stock_research.models import Snapshot
from stock_research.processes import isolated_environment, stop_process_group
from stock_research.quant_compute import QuantParameters, QuantReport, analysis_path, quant_parameters, verify_analysis
from stock_research.schemas.quant_execution import (
    MAX_QUANT_REPORT,
    MAX_QUANT_REQUEST,
    MAX_QUANT_RESPONSE,
    QUANT_TIMEOUT_SECONDS,
    QuantFinished,
    QuantRequest,
    QuantStarted,
)
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

logger = structlog.get_logger(__name__)


class QuantExecutionError(ResearchUnavailable):
    """只公开固定错误族，不转发子进程输出、路径或底层异常。"""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__("量化执行未完成：" + code)


async def quant_response(process: asyncio.subprocess.Process, request: QuantRequest) -> QuantFinished:
    """有界读取全部协议并等候终态，异常不会把运行中进程当成功。"""
    content = bytearray()
    while chunk := await process.stdout.read(4096):
        content.extend(chunk)
        if len(content) > MAX_QUANT_RESPONSE:
            raise QuantExecutionError("quant_output_invalid")
    await process.wait()
    try:
        first, last = bytes(content).splitlines()
        started, result = QuantStarted.model_validate_json(first), QuantFinished.model_validate_json(last)
        if (started.pid, started.strategy) != (process.pid, request.strategy):
            raise ValueError("量化启动绑定无效")
        if started.source_hash != request.source_hash:
            raise QuantExecutionError("quant_source_changed")
    except (ValueError, TypeError):
        raise QuantExecutionError("quant_output_invalid") from None
    if result.status != "completed":
        raise QuantExecutionError(result.error_code)
    if process.returncode != 0:
        raise QuantExecutionError("quant_process_failed")
    return result


async def execute_quant(request: QuantRequest, timeout: float) -> QuantFinished:
    """控制管道保持打开，子进程在父端异常死亡后自行终止。"""
    raw = (request.model_dump_json() + "\n").encode("utf-8")
    if len(raw) > MAX_QUANT_REQUEST:
        raise QuantExecutionError("quant_input_invalid")
    process = None
    try:
        async with asyncio.timeout(timeout):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "stock_research.quant_worker",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                cwd=request.output_path.parent,
                env=isolated_environment(),
                start_new_session=True,
            )
            process.stdin.write(raw)
            await process.stdin.drain()
            return await quant_response(process, request)
    except TimeoutError:
        raise QuantExecutionError("quant_timeout") from None
    except (OSError, ValueError):
        raise QuantExecutionError("quant_process_failed") from None
    finally:
        if process:
            cleanup = asyncio.create_task(stop_process_group(process))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                raise


def bounded_report(path: Path) -> str:
    """缓存和临时结果均有限额，文件存在不等于合法结果。"""
    if path.stat().st_size > MAX_QUANT_REPORT:
        raise QuantExecutionError("quant_output_invalid")
    return path.read_text(encoding="utf-8")


def publish_quant(path: Path, report: QuantReport, snapshot: Snapshot, parameters: QuantParameters) -> QuantReport:
    """并发同输入只采用已经完整校验的首份缓存，不覆盖成功产物。"""
    try:
        write_once(path, report.model_dump_json(indent=2) + "\n")
    except ValueError:
        if not path.is_file():
            raise
    return verify_analysis(bounded_report(path), snapshot, parameters)


async def analyze_isolated(
    settings: Settings,
    snapshot_path: Path,
    snapshot: Snapshot,
    strategy: str,
    *,
    timeout: float = QUANT_TIMEOUT_SECONDS,
    parameters: QuantParameters | None = None,
) -> QuantReport:
    """实际API与CLI run共用此边界，命中缓存仍重读校验。"""
    ensure_source_current()
    parameters = parameters or await asyncio.to_thread(quant_parameters, settings, strategy)
    path = await asyncio.to_thread(analysis_path, settings, snapshot, parameters)
    if await asyncio.to_thread(path.is_file):
        report = await asyncio.to_thread(
            verify_analysis, await asyncio.to_thread(bounded_report, path), snapshot, parameters
        )
        ensure_source_current()
        return report
    temporary = await asyncio.to_thread(TemporaryDirectory, prefix="research-quant-")
    try:
        request = QuantRequest(
            strategy=strategy,
            snapshot_path=snapshot_path.resolve(),
            snapshot_id=snapshot.id,
            snapshot_hash=snapshot.content_hash,
            source_hash=LOADED_SOURCE_IDENTITY,
            output_path=Path(temporary.name) / "result.json",
            parameters=parameters,
        )
        result = await execute_quant(request, timeout)
        report = await asyncio.to_thread(
            verify_analysis, await asyncio.to_thread(bounded_report, request.output_path), snapshot, parameters
        )
        if report.content_hash != result.content_hash:
            raise QuantExecutionError("quant_output_invalid")
        ensure_source_current()
        return await asyncio.to_thread(publish_quant, path, report, snapshot, parameters)
    except QuantExecutionError as error:
        logger.error("量化执行失败", strategy=strategy, error_code=error.code)
        raise
    finally:
        await asyncio.to_thread(temporary.cleanup)
