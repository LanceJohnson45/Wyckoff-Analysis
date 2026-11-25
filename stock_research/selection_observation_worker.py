"""正式初选观测隔离worker，父端EOF终止整个计算进程。"""

import os
import sys
import threading

import structlog

from core.trade_friction import configure_slippage
from stock_research.errors import ResearchSourceChanged
from stock_research.processes import control_closed
from stock_research.schemas.evaluation_execution import MAX_EVALUATION_REQUEST
from stock_research.schemas.selection_observation_execution import SelectionObservationFiles, SelectionObservationFrame
from stock_research.selection_observation_local import observe_files
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current


def emit(status: str, **fields: object) -> None:
    """stdout只输出有界类型帧，真实异常不进入业务回执。"""
    frame = SelectionObservationFrame(status=status, pid=os.getpid(), source_hash=LOADED_SOURCE_IDENTITY, **fields)
    sys.stdout.write(frame.model_dump_json() + "\n")
    sys.stdout.flush()


def main() -> int:
    """原初选范围和文件路径由父端固定，不加载模型或Personal身份。"""
    os.umask(0o077)
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
    try:
        raw = sys.stdin.buffer.readline(MAX_EVALUATION_REQUEST + 1)
        if len(raw) > MAX_EVALUATION_REQUEST or not raw.endswith(b"\n"):
            raise ValueError("观测输入协议越界")
        request = SelectionObservationFiles.model_validate_json(raw)
    except ValueError:
        emit("failed", error_code="evaluation_input_invalid")
        return 2
    threading.Thread(target=control_closed, daemon=True).start()
    emit("started")
    try:
        ensure_source_current()
        if request.source_hash != LOADED_SOURCE_IDENTITY:
            raise ResearchSourceChanged("观测源码已改变")
        configure_slippage(request.slippage_bps_per_side)
        result = observe_files(request, lambda fingerprint: emit("frozen", input_hash=fingerprint))
        emit(
            "completed",
            input_hash=result["input_hash"],
            report_hash=result["content_hash"],
            report_path=result["path"],
        )
        return 0
    except ResearchSourceChanged:
        error = "evaluation_source_changed"
    except (ValueError, OSError, TimeoutError):
        error = "evaluation_calculation_failed"
    except Exception:
        structlog.get_logger(__name__).exception("观测隔离计算异常")
        error = "evaluation_calculation_failed"
    emit("failed", error_code=error)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
