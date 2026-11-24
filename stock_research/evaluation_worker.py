"""一次批量评估的隔离worker；父端EOF不遗留后台计算。"""

import os
import sys
import threading

import structlog

from core.trade_friction import configure_slippage
from stock_research.errors import ResearchSourceChanged
from stock_research.evaluation_local import evaluate_files
from stock_research.processes import control_closed
from stock_research.schemas.evaluation_execution import MAX_EVALUATION_REQUEST, EvaluationFiles, EvaluationFrame
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current


def emit(status: str, **fields: object) -> None:
    """只输出验证后的有界协议，计算日志转入stderr。"""
    frame = EvaluationFrame(status=status, pid=os.getpid(), source_hash=LOADED_SOURCE_IDENTITY, **fields)
    sys.stdout.write(frame.model_dump_json() + "\n")
    sys.stdout.flush()


def main() -> int:
    """不实例化Settings或读取模型、数据库、Personal配置。"""
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
    try:
        raw = sys.stdin.buffer.readline(MAX_EVALUATION_REQUEST + 1)
        if len(raw) > MAX_EVALUATION_REQUEST or not raw.endswith(b"\n"):
            raise ValueError("评估输入协议越界")
        request = EvaluationFiles.model_validate_json(raw)
    except ValueError:
        emit("failed", error_code="evaluation_input_invalid")
        return 2
    threading.Thread(target=control_closed, daemon=True).start()
    emit("started")
    try:
        ensure_source_current()
        if request.source_hash != LOADED_SOURCE_IDENTITY:
            raise ResearchSourceChanged("评估源码已改变")
        configure_slippage(request.slippage_bps_per_side)
        result = evaluate_files(
            request,
            checkpoint=lambda status, count, fingerprint: emit(
                status,
                completed_sections=count,
                input_hash=fingerprint,
            ),
        )
        emit(
            "completed",
            completed_sections=20,
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
        structlog.get_logger(__name__).exception("评估隔离计算异常")
        error = "evaluation_calculation_failed"
    emit("failed", error_code=error)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
