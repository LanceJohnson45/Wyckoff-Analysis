"""受控子进程的环境、父端控制管道及有界进程组回收。"""

import asyncio
import os
import signal
import sys
from pathlib import Path


def isolated_environment() -> dict[str, str]:
    """默认不继承数据库、API、模型、Personal身份或用户配置目录。"""
    return {
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
        "PYTHONUNBUFFERED": "1",
        "PYTHONFAULTHANDLER": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "OMP_NUM_THREADS": "1",
        "VECLIB_MAXIMUM_THREADS": "1",
        **{key: os.environ[key] for key in ("LANG", "LC_ALL", "TZ") if key in os.environ},
    }


def control_closed() -> None:
    """独立线程在父端死亡或越界输入时终止本次进程，不遗留后台工作。"""
    try:
        os.read(sys.stdin.fileno(), 1)
    finally:
        os._exit(75)


async def stop_process_group(process: asyncio.subprocess.Process) -> None:
    """只回收本次创建的进程组，wait成功前不视为停止。"""
    try:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), 2)
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await asyncio.wait_for(process.wait(), 3)
    except ProcessLookupError:
        await asyncio.wait_for(process.wait(), 3)
    finally:
        if process.stdin:
            process.stdin.close()
