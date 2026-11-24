"""一次固定输入的量化进程；控制管道关闭时不留下孤儿计算。"""

import json
import os
import sys
import threading

import structlog

from stock_research.errors import ResearchSourceChanged
from stock_research.models import Snapshot
from stock_research.processes import control_closed
from stock_research.quant_compute import compute_analysis
from stock_research.schemas.quant_execution import (
    MAX_QUANT_REPORT,
    MAX_QUANT_REQUEST,
    MAX_QUANT_SNAPSHOT,
    QuantFinished,
    QuantRequest,
    QuantStarted,
)
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

logger = structlog.get_logger(__name__)


def worker_compute(request: QuantRequest) -> str:
    """只计算已绑定固定文件；不实例化Settings或读取任何身份配置。"""
    ensure_source_current()
    if request.source_hash != LOADED_SOURCE_IDENTITY:
        raise ResearchSourceChanged("量化子进程源码与父进程不一致")
    if request.snapshot_path.stat().st_size > MAX_QUANT_SNAPSHOT:
        raise ValueError("量化快照超出大小上限")
    snapshot = Snapshot.model_validate_json(request.snapshot_path.read_text(encoding="utf-8"))
    if snapshot.id != request.snapshot_id or snapshot.content_hash != request.snapshot_hash:
        raise ValueError("量化快照绑定不一致")
    report = compute_analysis(snapshot, request.parameters)
    raw = report.model_dump_json()
    if len(raw.encode("utf-8")) > MAX_QUANT_REPORT:
        raise ValueError("量化结果超出大小上限")
    ensure_source_current()
    descriptor = os.open(request.output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    return report.content_hash


def main() -> int:
    """stdout仅承载两帧有界协议，业务诊断不进入返回值。"""
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))
    try:
        raw = sys.stdin.buffer.readline(MAX_QUANT_REQUEST + 1)
        if len(raw) > MAX_QUANT_REQUEST or not raw.endswith(b"\n"):
            raise ValueError("量化请求帧无效")
        request = QuantRequest.model_validate_json(raw)
    except (ValueError, OSError):
        return 2
    threading.Thread(target=control_closed, daemon=True).start()
    started = QuantStarted(
        status="started", pid=os.getpid(), source_hash=LOADED_SOURCE_IDENTITY, strategy=request.strategy
    )
    sys.stdout.write(started.model_dump_json() + "\n")
    sys.stdout.flush()
    try:
        content_hash = worker_compute(request)
        result = QuantFinished(status="completed", content_hash=content_hash, error_code=None)
    except Exception as error:
        code = "quant_source_changed" if isinstance(error, ResearchSourceChanged) else "quant_calculation_failed"
        logger.error("量化子进程失败", error_type=type(error).__name__)
        result = QuantFinished(status="failed", content_hash=None, error_code=code)
    sys.stdout.write(json.dumps(result.model_dump(mode="json")) + "\n")
    sys.stdout.flush()
    return 0 if result.status == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
