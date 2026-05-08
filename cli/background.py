# -*- coding: utf-8 -*-
"""后台任务管理器 — 长任务非阻塞执行。"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class BackgroundTask:
    id: str
    tool_name: str
    status: str = "pending"          # pending → running → completed | failed
    result: Any = None
    error: str = ""
    submitted_at: float = field(default_factory=time.monotonic)
    completed_at: float | None = None


class BackgroundTaskManager:
    """线程安全的后台任务管理器。"""

    def __init__(self):
        self._tasks: dict[str, BackgroundTask] = {}
        self._lock = threading.Lock()

    def submit(
        self,
        task_id: str,
        tool_name: str,
        fn: Callable,
        args: dict[str, Any],
        on_complete: Callable[[str, str, Any], None] | None = None,
    ) -> str:
        task = BackgroundTask(id=task_id, tool_name=tool_name)
        with self._lock:
            self._tasks[task_id] = task

        def _run():
            with self._lock:
                task.status = "running"
            try:
                result = fn(**args)
                with self._lock:
                    task.result = result
                    task.status = "completed"
                    task.completed_at = time.monotonic()
                if on_complete:
                    on_complete(task_id, tool_name, result)
            except Exception as e:
                logger.exception("Background task %s failed", task_id)
                with self._lock:
                    task.error = str(e)
                    task.status = "failed"
                    task.completed_at = time.monotonic()
                if on_complete:
                    on_complete(task_id, tool_name, {"error": str(e)})

        t = threading.Thread(target=_run, daemon=True, name=f"bg-{task_id}")
        t.start()
        return task_id

    def get_status(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            task = self._tasks.get(task_id)
        if task is None:
            return None
        elapsed = (task.completed_at or time.monotonic()) - task.submitted_at
        return {
            "task_id": task.id,
            "tool_name": task.tool_name,
            "status": task.status,
            "elapsed": f"{elapsed:.0f}s",
            "error": task.error or None,
        }

    def list_tasks(self) -> list[dict[str, Any]]:
        with self._lock:
            tasks = list(self._tasks.values())
        return [self.get_status(t.id) for t in tasks if self.get_status(t.id)]
