"""一次只读行情同步和显式基准补充；身份仅由父端匿名管道授予。"""

import asyncio
import os
import sys
import threading
from contextlib import redirect_stdout

import structlog

from stock_research.artifacts import data_lock
from stock_research.benchmarks import fetch_benchmarks
from stock_research.identity import now
from stock_research.market_sync import sync_market
from stock_research.processes import control_closed
from stock_research.schemas.market_refresh import MAX_REFRESH_FRAME, RefreshEvent, RefreshRequest, SyncSummary
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

logger = structlog.get_logger(__name__)


def emit(request: RefreshRequest, kind: str, **values: object) -> None:
    """只输出无身份的有界协议，provider输出另送stderr。"""
    event = RefreshEvent(kind=kind, id=request.id, source_hash=LOADED_SOURCE_IDENTITY, pid=os.getpid(), **values)
    sys.stdout.write(event.model_dump_json() + "\n")
    sys.stdout.flush()


async def update_market(request: RefreshRequest) -> int:
    """同步已确认后立即发布该步回执，基准失败不掩盖已完成的本机同步。"""
    phase = "refresh_sync_failed"
    try:
        ensure_source_current()
        if request.source_hash != LOADED_SOURCE_IDENTITY:
            emit(request, "finished", error_code="refresh_source_changed")
            return 1
        remaining = (request.deadline - now()).total_seconds()
        async with asyncio.timeout(max(0, remaining)):
            with data_lock(request.benchmark_directory.parent / "locks", "us", str(request.as_of)):
                result = await sync_market(request.source, request.local_path)
            emit(request, "synced", sync=SyncSummary.model_validate(result))
            output = {}
            if request.fetch_benchmarks:
                phase = "refresh_benchmark_failed"
                with redirect_stdout(sys.stderr):
                    output = await asyncio.to_thread(
                        fetch_benchmarks, request.as_of, request.history_rows, request.benchmark_directory
                    )
            ensure_source_current()
            emit(request, "finished", benchmark_path=output.get("path"), benchmark_hash=output.get("content_hash"))
            return 0
    except TimeoutError:
        emit(request, "finished", error_code="refresh_timeout")
    except Exception as error:
        logger.error("行情子进程未完成", phase=phase, error_type=type(error).__name__)
        emit(request, "finished", error_code=phase)
    return 1


def main() -> int:
    """控制管道保持打开，父进程死亡时立即停止本次更新。"""
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
    try:
        raw = sys.stdin.buffer.readline(MAX_REFRESH_FRAME + 1)
        if len(raw) > MAX_REFRESH_FRAME or not raw.endswith(b"\n"):
            return 2
        request = RefreshRequest.model_validate_json(raw)
    except (ValueError, OSError):
        return 2
    threading.Thread(target=control_closed, daemon=True).start()
    emit(request, "started")
    return asyncio.run(update_market(request))


if __name__ == "__main__":
    raise SystemExit(main())
