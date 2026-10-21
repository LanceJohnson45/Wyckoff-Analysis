"""真实子进程验证超时终止、队列续跑与全局限频，不连接行情或数据库。"""

import multiprocessing as mp
import os
from pathlib import Path
import signal
from threading import Event
import time

import pytest

from scripts.prewarm_workers import PrewarmBudgetExpired, iter_bounded_jobs


_rate_connection = None


def _rate_initializer(connection) -> None:
    global _rate_connection
    _rate_connection = connection


def _job(mode: str, path: str = "") -> tuple[int, float]:
    if path:
        Path(path).write_text(str(os.getpid()))
    if mode == "hang":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        Event().wait(30)
    elif mode == "slow":
        Event().wait(0.6)
    elif mode == "rate":
        _rate_connection.send(("rate_limit", 0.2))
        reserved = _rate_connection.recv()
        Event().wait(max(reserved - time.monotonic(), 0))
    return os.getpid(), time.monotonic()


def _production_rate_job() -> float:
    from integrations import data_source

    data_source._wait_tickflow_daily_slot()
    first = time.monotonic()
    data_source._wait_tickflow_daily_slot()
    return time.monotonic() - first


def test_normal_production_worker_keeps_tickflow_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    """【正常值】真实生产初始化函数在spawn进程中接入父进程限频。"""
    from scripts.funnel_prewarm import _configure_prewarm_worker

    monkeypatch.setenv("TICKFLOW_DAILY_MIN_INTERVAL_SECONDS", "0.2")
    started = time.monotonic()

    outcomes = dict(iter_bounded_jobs(_production_rate_job, [("rate", (), {})], max_workers=1,
                                      task_timeout=15, dispatch_deadline=started + 18,
                                      run_deadline=started + 20, initializer=_configure_prewarm_worker))

    assert outcomes["rate"].result() >= 0.15


def test_error_hung_process_is_killed_and_next_stock_finishes(tmp_path: Path) -> None:
    """【异常值】无响应且忽略SIGTERM的股票被强杀回收，下一只正常完成。"""
    before = {child.pid for child in mp.active_children()}
    pid_file = tmp_path / "hung.pid"
    started = time.monotonic()
    jobs = [("hung", ("hang", str(pid_file)), {}), ("healthy", ("fast",), {})]

    outcomes = dict(iter_bounded_jobs(_job, jobs, max_workers=1, task_timeout=3,
                                      dispatch_deadline=started + 12, run_deadline=started + 14))

    with pytest.raises(TimeoutError, match="symbol timeout"):
        outcomes["hung"].result()
    assert outcomes["healthy"].result()[0] != int(pid_file.read_text())
    assert time.monotonic() - started < 10
    assert {child.pid for child in mp.active_children()} == before
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


def test_boundary_dispatch_deadline_leaves_queued_stock_unstarted(tmp_path: Path) -> None:
    """【边界值】停止派发后在途股票完成，剩余股票不会运行或写数据。"""
    untouched = tmp_path / "must-not-start"
    started = time.monotonic()
    jobs = [("active", ("slow",), {}), ("pending", ("fast", str(untouched)), {})]

    outcomes = dict(iter_bounded_jobs(_job, jobs, max_workers=1, task_timeout=4,
                                      dispatch_deadline=started + 0.3, run_deadline=started + 5))

    assert outcomes["active"].result()[0] > 0
    with pytest.raises(PrewarmBudgetExpired):
        outcomes["pending"].result()
    assert not untouched.exists()


def test_boundary_run_deadline_kills_inflight_process(tmp_path: Path) -> None:
    """【边界值】批次收尾时间到达时终止在途任务，父进程不会等到单票上限。"""
    started = time.monotonic()
    jobs = [("hung", ("hang", str(tmp_path / "pid")), {})]

    outcomes = dict(iter_bounded_jobs(_job, jobs, max_workers=1, task_timeout=20,
                                      dispatch_deadline=started + 0.2, run_deadline=started + 2))

    with pytest.raises(PrewarmBudgetExpired):
        outcomes["hung"].result()
    assert time.monotonic() - started < 7


def test_normal_workers_reuse_processes_and_share_rate_limit() -> None:
    """【正常值】两个执行槽复用进程，四次请求仍遵守父进程的统一间隔。"""
    started = time.monotonic()
    jobs = [(str(i), ("rate",), {}) for i in range(4)]

    outcomes = list(iter_bounded_jobs(_job, jobs, max_workers=2, task_timeout=5,
                                     dispatch_deadline=started + 10, run_deadline=started + 12,
                                     initializer=_rate_initializer))
    values = [future.result() for _, future in outcomes]

    assert len({pid for pid, _ in values}) <= 2
    times = sorted(timestamp for _, timestamp in values)
    assert all(b - a >= 0.15 for a, b in zip(times, times[1:]))


def test_boundary_dispatch_pacing_does_not_delay_timeout_supervision() -> None:
    """【边界值】派发间隔大于单票时限时，仍及时强杀卡住的任务。"""
    started = time.monotonic()
    jobs = [("hung", ("hang",), {}), ("healthy", ("fast",), {})]
    completed = []

    for symbol, future in iter_bounded_jobs(_job, jobs, max_workers=1, task_timeout=1,
                                            dispatch_deadline=started + 7, run_deadline=started + 8,
                                            dispatch_interval=3):
        completed.append((symbol, future, time.monotonic() - started))

    assert completed[0][0] == "hung" and completed[0][2] < 2.8
    with pytest.raises(TimeoutError):
        completed[0][1].result()
    assert completed[1][0] == "healthy"
    assert completed[1][1].result()[0] > 0
