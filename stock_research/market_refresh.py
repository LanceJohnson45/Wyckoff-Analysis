"""行情子进程监督及回执校验；没有云端写入或模型执行能力。"""

import asyncio
import json
import sys
from collections.abc import Awaitable, Callable
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from stock_research.benchmarks import BenchmarkBundle
from stock_research.config import MarketSourceSettings, Settings
from stock_research.errors import ResearchUnavailable
from stock_research.identity import new_id, now
from stock_research.processes import isolated_environment, stop_process_group
from stock_research.schemas.market_refresh import (
    MAX_REFRESH_FRAME,
    RefreshErrorCode,
    RefreshEvent,
    RefreshReceipt,
    RefreshRequest,
)
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

RefreshCheckpoint = Callable[[RefreshReceipt], Awaitable[None]]


class RefreshExecutionError(ResearchUnavailable):
    """回执只含固定错误族，不包含驱动异常、连接或私有路径。"""

    def __init__(self, receipt: RefreshReceipt) -> None:
        self.receipt = receipt
        super().__init__("行情更新未完成：" + receipt.error_code)


def validated_benchmark(request: RefreshRequest, result: RefreshEvent) -> Path | None:
    """基准文件必须是授予目录内本次截止日的完整不可变版本。"""
    if not request.fetch_benchmarks:
        if result.benchmark_path is not None or result.benchmark_hash is not None:
            raise ValueError("未启用供应商补充，不能接收基准文件")
        return None
    path = result.benchmark_path
    if (
        path is None
        or result.benchmark_hash is None
        or path.is_symlink()
        or not path.is_file()
        or path.parent != request.benchmark_directory
        or path.name != result.benchmark_hash + ".json"
        or path.stat().st_size > 64 * 1024 * 1024
    ):
        raise ValueError("基准补充路径或大小无效")
    bundle = BenchmarkBundle.model_validate_json(path.read_text(encoding="utf-8"))
    if (bundle.content_hash, bundle.as_of, bundle.history_rows) != (
        result.benchmark_hash,
        request.as_of,
        request.history_rows,
    ):
        raise ValueError("基准补充与本次更新不一致")
    return path


async def read_refresh(
    process: asyncio.subprocess.Process,
    request: RefreshRequest,
    progress: list[RefreshReceipt],
    checkpoint: RefreshCheckpoint,
) -> tuple[RefreshReceipt, Path | None]:
    """严格三步协议；同步进度先落库，最终成功等待真实退出和产物重读。"""
    receipt = progress[-1]
    frames = []
    while raw := await process.stdout.readline():
        if len(raw) > MAX_REFRESH_FRAME or len(frames) >= 3:
            raise ValueError("行情更新协议超限")
        event = RefreshEvent.model_validate_json(raw)
        if (event.id, event.pid, event.source_hash) != (request.id, process.pid, request.source_hash):
            raise ValueError("行情更新进程绑定无效")
        expected = "started" if not frames else "synced" if len(frames) == 1 else "finished"
        if event.kind != expected and not (len(frames) == 1 and event.kind == "finished" and event.error_code):
            raise ValueError("行情更新协议次序无效")
        frames.append(event)
        if event.kind == "synced":
            if event.sync is None or event.error_code:
                raise ValueError("行情同步未确认结果")
            receipt = receipt.model_copy(update={"sync": event.sync})
            progress.append(receipt)
            await checkpoint(receipt)
    await process.wait()
    if not frames or frames[-1].kind != "finished":
        raise ValueError("行情更新缺少终态")
    result = frames[-1]
    if result.error_code:
        raise RefreshExecutionError(
            receipt.model_copy(
                update={
                    "status": "failed",
                    "error_code": result.error_code,
                    "finished_at": now(),
                }
            )
        )
    if process.returncode or receipt.sync is None:
        raise ValueError("行情更新未成功退出")
    path = validated_benchmark(request, result)
    ensure_source_current()
    return receipt.model_copy(
        update={
            "status": "completed",
            "finished_at": now(),
            "benchmark_hash": result.benchmark_hash,
        }
    ), path


async def execute_refresh(
    request: RefreshRequest,
    receipt: RefreshReceipt,
    checkpoint: RefreshCheckpoint,
    directory: Path,
) -> tuple[RefreshReceipt, Path | None]:
    """身份只经匿名管道，SIGTERM/超时后确认进程组回收才保存终态。"""
    wire = request.model_dump(mode="json")
    wire["source"]["password"] = request.source.password.get_secret_value()
    raw = (json.dumps(wire) + "\n").encode("utf-8")
    process = None
    code: RefreshErrorCode | None = None
    result = None
    progress = [receipt]
    try:
        if len(raw) > MAX_REFRESH_FRAME:
            raise ValueError("行情更新请求超限")
        async with asyncio.timeout(max(0, (request.deadline - now()).total_seconds())):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "stock_research.market_refresh_worker",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                cwd=directory,
                env={**isolated_environment(), "HOME": str(directory)},
                start_new_session=True,
                limit=MAX_REFRESH_FRAME,
            )
            process.stdin.write(raw)
            await process.stdin.drain()
            result = await read_refresh(process, request, progress, checkpoint)
    except asyncio.CancelledError:
        code = "refresh_cancelled"
    except TimeoutError:
        code = "refresh_timeout"
    except RefreshExecutionError as error:
        progress.append(error.receipt)
        code = error.receipt.error_code
    except (OSError, ValueError):
        code = "refresh_protocol_invalid" if process else "refresh_process_failed"
    finally:
        if process:
            cleanup = asyncio.create_task(stop_process_group(process))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await cleanup
                code = "refresh_cancelled"
    if code:
        receipt = progress[-1].model_copy(
            update={
                "status": "cancelled" if code == "refresh_cancelled" else "failed",
                "error_code": code,
                "finished_at": now(),
            }
        )
        await checkpoint(receipt)
        if code == "refresh_cancelled":
            raise asyncio.CancelledError
        raise RefreshExecutionError(receipt)
    await checkpoint(result[0])
    return result


async def refresh_market(
    settings: Settings,
    as_of: date,
    *,
    timeout: float,
    checkpoint: RefreshCheckpoint,
    source: MarketSourceSettings | None = None,
) -> Settings:
    """只对本次准备替换基准路径，不修改operator配置或旧研究。"""
    ensure_source_current()
    receipt = RefreshReceipt(
        id=new_id(),
        as_of=as_of,
        source_hash=LOADED_SOURCE_IDENTITY,
        status="unconfirmed",
        started_at=now(),
        finished_at=None,
        sync=None,
        benchmark_hash=None,
        error_code=None,
    )
    await checkpoint(receipt)
    try:
        request = RefreshRequest(
            id=receipt.id,
            source_hash=receipt.source_hash,
            as_of=as_of,
            history_rows=settings.history_rows,
            local_path=settings.local_market_path.expanduser().resolve(),
            benchmark_directory=(settings.artifacts_path / "benchmarks").expanduser().resolve(),
            fetch_benchmarks=settings.benchmark_path is not None,
            deadline=now() + timedelta(seconds=timeout),
            source=source or MarketSourceSettings(),
        )
    except ValueError:
        failed = receipt.model_copy(
            update={
                "status": "failed",
                "finished_at": now(),
                "error_code": "refresh_configuration_invalid",
            }
        )
        await checkpoint(failed)
        raise RefreshExecutionError(failed) from None
    root = settings.artifacts_path / "market-jobs"
    root.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=root, prefix="refresh-") as directory:
        _, path = await execute_refresh(request, receipt, checkpoint, Path(directory))
    return settings.model_copy(update={"benchmark_path": path}) if path else settings


async def refresh_command(settings: Settings, as_of: date) -> tuple[dict[str, object], int]:
    """显式CLI更新只回报本次步骤，不能把传输完成当成研究质量通过。"""
    receipts = []

    async def collect(receipt: RefreshReceipt) -> None:
        receipts.append(receipt)

    try:
        updated = await refresh_market(
            settings, as_of, timeout=settings.market_refresh_timeout_seconds, checkpoint=collect
        )
    except RefreshExecutionError as error:
        return {"refresh": error.receipt.model_dump(mode="json")}, 2
    receipt = receipts[-1]
    return {
        "refresh": receipt.model_dump(mode="json"),
        "benchmark_path": str(updated.benchmark_path) if updated.benchmark_path else None,
        "quality_checked": False,
    }, 2 if receipt.sync.issues or receipt.sync.rejected else 0
