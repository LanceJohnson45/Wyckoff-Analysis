"""预热任务的进程隔离、单项硬超时及批次派发预算。"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future
from contextlib import closing
from dataclasses import dataclass
import multiprocessing as mp
from multiprocessing.connection import Connection, wait
import time
from threading import Event
from typing import Any


class PrewarmBudgetExpired(TimeoutError):
    """批次预算已用完；任务留待下一轮处理。"""


def _worker(connection: Connection, function: Callable[..., Any], initializer: Callable | None) -> None:
    """在独立进程内顺序执行任务，连接与数据库资源不从父进程继承。"""
    try:
        if initializer is not None:
            initializer(connection)
        while True:
            args, kwargs = connection.recv()
            try:
                connection.send((True, function(*args, **kwargs)))
            except Exception as exc:
                connection.send((False, f"{type(exc).__name__}: {exc}"))
    except (EOFError, BrokenPipeError):
        pass
    finally:
        connection.close()


@dataclass
class _Slot:
    """父进程持有的执行槽；每个子进程有独立通信管道。"""

    process: Any
    connection: Connection
    symbol: str = ""
    started: float = 0.0

    def stop(self) -> None:
        """先终止，必要时强杀并回收，禁止遗留后台行情请求。"""
        self.connection.close()
        if self.process.is_alive():
            self.process.terminate()
        self.process.join(timeout=0.5)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=0.5)
        if self.process.is_alive():
            raise RuntimeError(f"无法回收预热子进程 {self.process.pid}")
        self.process.close()


def _future(value: Any = None, error: Exception | None = None) -> Future:
    result = Future()
    if error is not None:
        result.set_exception(error)
    else:
        result.set_result(value)
    return result


def iter_bounded_jobs(
    function: Callable[..., Any],
    jobs: Iterable[tuple[str, tuple, dict]],
    *,
    max_workers: int,
    task_timeout: float,
    dispatch_deadline: float,
    run_deadline: float,
    initializer: Callable | None = None,
    dispatch_interval: float = 0,
) -> Iterator[tuple[str, Future]]:
    """惰性派发任务；超时杀掉对应进程，正常任务复用执行槽。

    Args:
        function: 可被spawn导入的模块级函数。
        jobs: 标识、位置参数和关键字参数组成的任务序列。
        max_workers: 同时在途的任务上限。
        task_timeout: 单次任务总时限（秒，含进程首次启动）。
        dispatch_deadline: 单调时钟上的停止派发时间。
        run_deadline: 单调时钟上的强制收尾时间。
        initializer: 子进程创建后的资源初始化函数。
        dispatch_interval: 新任务的最小派发间隔，等待期间继续监督在途超时。

    Yields:
        已完成或已延期的标识与Future；父进程负责检查点及审核写出。
    """
    if max_workers < 1 or task_timeout <= 0 or dispatch_interval < 0 or dispatch_deadline > run_deadline:
        raise ValueError("预热并发、超时或预算配置无效")
    context = mp.get_context("spawn")
    queue = iter(jobs)
    slots: list[_Slot] = []
    exhausted = False
    next_rate_slot = 0.0
    next_dispatch = 0.0
    try:
        while True:
            for slot in list(slots):
                if not slot.symbol and not slot.process.is_alive():
                    slot.stop()
                    slots.remove(slot)
            while not exhausted and time.monotonic() < dispatch_deadline:
                if time.monotonic() < next_dispatch:
                    break
                idle = next((slot for slot in slots if not slot.symbol), None)
                if idle is None and len(slots) >= max_workers:
                    break
                try:
                    symbol, args, kwargs = next(queue)
                except StopIteration:
                    exhausted = True
                    break
                if idle is None:
                    parent, child = context.Pipe()
                    process = context.Process(target=_worker, args=(child, function, initializer), daemon=True)
                    process.start()
                    child.close()
                    idle = _Slot(process, parent)
                    slots.append(idle)
                idle.symbol = symbol
                idle.started = time.monotonic()
                next_dispatch = idle.started + dispatch_interval
                try:
                    idle.connection.send((args, kwargs))
                except (BrokenPipeError, EOFError, OSError) as exc:
                    idle.stop()
                    slots.remove(idle)
                    yield symbol, _future(error=RuntimeError(f"预热进程派发失败: {exc}"))

            active = [slot for slot in slots if slot.symbol]
            if not active:
                if not exhausted and time.monotonic() < dispatch_deadline:
                    Event().wait(min(max(next_dispatch - time.monotonic(), 0), 0.1))
                    continue
                break
            ready = wait([slot.connection for slot in active], timeout=0.1)
            for slot in active:
                symbol = slot.symbol
                now = time.monotonic()
                error: Exception | None = None
                if slot.connection in ready:
                    try:
                        success, value = slot.connection.recv()
                        if success == "rate_limit":
                            reserved = max(time.monotonic(), next_rate_slot)
                            next_rate_slot = reserved + float(value)
                            slot.connection.send(reserved)
                            continue
                        future = _future(value) if success else _future(error=RuntimeError(value))
                        slot.symbol = ""
                        yield symbol, future
                        continue
                    except (EOFError, BrokenPipeError, OSError) as exc:
                        error = RuntimeError(f"预热子进程异常退出: {exc}")
                elif now >= run_deadline:
                    error = PrewarmBudgetExpired("batch deadline: 在途任务已终止，下一轮重新核验")
                elif now - slot.started >= task_timeout:
                    error = TimeoutError(f"symbol timeout: {symbol} exceeded {task_timeout:g}s")
                elif not slot.process.is_alive():
                    error = RuntimeError(f"预热子进程退出码 {slot.process.exitcode}")
                if error is not None:
                    slot.stop()
                    slots.remove(slot)
                    yield symbol, _future(error=error)
        if not exhausted:
            for symbol, _, _ in queue:
                yield symbol, _future(error=PrewarmBudgetExpired("batch deadline: 未派发，下一轮续跑"))
    finally:
        for slot in slots:
            slot.stop()


def bounded_call(function: Callable[..., Any], *args: Any, timeout: float, **kwargs: Any) -> Any:
    """给名单、交易日历等批次准备操作设置同样可终止的时间上限。"""
    deadline = time.monotonic() + timeout
    with closing(iter_bounded_jobs(function, [("prepare", args, kwargs)], max_workers=1,
                                   task_timeout=timeout, dispatch_deadline=deadline, run_deadline=deadline)) as outcomes:
        for _, future in outcomes:
            return future.result()
    raise PrewarmBudgetExpired("准备阶段预算不足")
