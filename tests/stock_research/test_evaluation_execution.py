"""真实评估子进程的协议、取消、父端EOF和结果绑定。"""

import asyncio
import os
import signal
import sys

import pytest

from stock_research.evaluation_execution import EvaluationExecutionError, execute_evaluation
from stock_research.processes import isolated_environment, stop_process_group
from stock_research.schemas.evaluation_execution import EvaluationFiles, EvaluationFrame


def test_integration_evaluation_worker_completes_twenty_sections(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 实际worker无身份配置运行正式两策略并返回固定产物。"""

    async def scenario() -> None:
        events = []

        async def checkpoint(frame: EvaluationFrame) -> None:
            events.append(frame)

        report = await execute_evaluation(evaluation_files, timeout=30, checkpoint=checkpoint)

        assert len(report.sections) == 40
        assert [frame.completed_sections for frame in events if frame.status == "section"] == list(range(1, 21))
        assert events[0].status == "started" and events[-1].status == "completed"
        with pytest.raises(ProcessLookupError):
            os.kill(events[0].pid, 0)

    asyncio.run(scenario())


def test_integration_evaluation_worker_cancel_waits_for_exit(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 父端取消不能只改任务状态，所属worker必须已退出。"""

    async def scenario() -> None:
        started = asyncio.Event()
        pids = []

        async def checkpoint(frame: EvaluationFrame) -> None:
            if frame.status == "started":
                pids.append(frame.pid)
                started.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(execute_evaluation(evaluation_files, timeout=30, checkpoint=checkpoint))
        await asyncio.wait_for(started.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        with pytest.raises(ProcessLookupError):
            os.kill(pids[0], 0)

    asyncio.run(scenario())


def test_integration_evaluation_worker_eof_and_interruption(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 控制管道关闭或实际信号中断均不遗留孤儿计算。"""

    async def scenario() -> None:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "stock_research.evaluation_worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=isolated_environment(),
            start_new_session=True,
        )
        try:
            process.stdin.write((evaluation_files.model_dump_json() + "\n").encode())
            await process.stdin.drain()
            first = EvaluationFrame.model_validate_json(await asyncio.wait_for(process.stdout.readline(), 10))
            assert first.status == "started"
            process.stdin.close()
            assert await asyncio.wait_for(process.wait(), 10) == 75
        finally:
            await stop_process_group(process)

        async def interrupt(frame: EvaluationFrame) -> None:
            if frame.status == "started":
                os.kill(frame.pid, signal.SIGTERM)

        with pytest.raises(EvaluationExecutionError) as error:
            await execute_evaluation(evaluation_files, timeout=30, checkpoint=interrupt)
        assert error.value.code == "evaluation_process_failed"

    asyncio.run(scenario())


def test_integration_evaluation_worker_timeout_and_wrong_source(evaluation_files: EvaluationFiles) -> None:
    """[INTEGRATION] 原预算超时和源码不匹配明确失败，不发布成功结果。"""

    async def scenario() -> None:
        with pytest.raises(EvaluationExecutionError) as timeout:
            await execute_evaluation(evaluation_files, timeout=0.001)
        assert timeout.value.code == "evaluation_timeout"
        with pytest.raises(EvaluationExecutionError) as source:
            await execute_evaluation(evaluation_files.model_copy(update={"source_hash": "0" * 64}), timeout=10)
        assert source.value.code == "evaluation_source_changed"

    asyncio.run(scenario())
