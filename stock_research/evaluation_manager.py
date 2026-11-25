"""有限评估任务接单、隔离执行、持久化取消和重启恢复。"""

import asyncio
from collections.abc import Mapping
from datetime import datetime, timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchInputError, ResearchSourceChanged, ResearchUnavailable
from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_configuration import freeze_evaluation
from stock_research.evaluation_execution import EvaluationExecutionError, execute_evaluation
from stock_research.evaluation_results import publish_evaluation
from stock_research.evaluation_store import EvaluationStore
from stock_research.identity import now
from stock_research.run_store import RunStore
from stock_research.schema import EVALUATION_JOBS
from stock_research.schemas.evaluation_execution import EvaluationFiles, EvaluationFrame
from stock_research.schemas.evaluations import CreateEvaluation
from stock_research.schemas.selection_observation_execution import SelectionObservationFiles, SelectionObservationFrame
from stock_research.schemas.selection_observation_jobs import CreateSelectionObservation
from stock_research.schemas.selection_observations import SelectionObservationReport
from stock_research.selection_observation_execution import execute_selection_observation
from stock_research.selection_observation_results import freeze_selection_observation, publish_selection_observation
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

logger = structlog.get_logger(__name__)


class EvaluationManager:
    """每个workspace最多一批评估，与研究/问答共享实例接单容量。"""

    def __init__(
        self,
        store: RunStore,
        settings: Settings,
        *,
        admission: asyncio.Lock,
        peer_tasks: Mapping[str, asyncio.Task[None]],
    ) -> None:
        self.store, self.settings, self.admission = store, settings, admission
        self.jobs, self.peer_tasks = EvaluationStore(store), peer_tasks
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.watcher: asyncio.Task[None] | None = None
        self.closing = False

    async def submit(self, key: str, body: CreateEvaluation | CreateSelectionObservation) -> str:
        """先落原意图和总期限，未知响应重放原身份，不重新消费配置。"""
        ensure_source_current()
        if body.cutoff > latest_complete_session():
            raise ResearchInputError("评估截止日尚未完成正式收盘")
        try:
            sessions_ending(body.cutoff, 1)
        except ValueError:
            raise ResearchInputError("评估截止日必须是XNYS交易日") from None
        async with self.admission:
            if self.closing:
                raise ResearchUnavailable("评估服务正在关闭")
            row = await self.jobs.request(key, body)
            if row is None:
                if len(self.tasks) + len(self.peer_tasks) >= 2:
                    raise ResearchConflict("研究任务容量已满")
                row = await self.jobs.create(
                    key,
                    body,
                    {
                        "source_hash": LOADED_SOURCE_IDENTITY,
                        "deadline": (now() + timedelta(seconds=self.settings.run_timeout_seconds)).isoformat(),
                    },
                )
            if row["status"] in {"pending", "running"}:
                self.start(row["uuid"])
            return row["uuid"]

    def start(self, identifier: str) -> None:
        """保留执行引用，恢复队列最多一项，不突破共享容量。"""
        if self.closing or identifier in self.tasks or len(self.tasks) + len(self.peer_tasks) >= 2:
            return
        ensure_source_current()
        task = asyncio.create_task(self.run(identifier))
        self.tasks[identifier] = task
        task.add_done_callback(lambda done: self.tasks.pop(identifier, None))

    async def run(self, identifier: str) -> None:
        """先取得原生PG执行权；其他拥有者的任务不被误标失败。"""
        try:
            async with self.store.execution_lock(identifier, table=EVALUATION_JOBS):
                try:
                    await self.run_locked(identifier)
                except asyncio.CancelledError:
                    await self.finish_cancel(identifier)
                    raise
                except EvaluationExecutionError as error:
                    await self.finish_error(identifier, error.code)
                except ResearchSourceChanged:
                    await self.finish_error(identifier, "evaluation_source_changed")
                except (ValueError, ResearchUnavailable, OSError, SQLAlchemyError, TimeoutError):
                    current = await self.jobs.read(identifier)
                    if current["payload"].get("cancel_requested"):
                        await self.finish_cancel(identifier)
                    else:
                        await self.finish_error(identifier, "evaluation_prepare_failed")
                except Exception:
                    logger.exception("评估执行异常", evaluation_id=identifier)
                    await self.finish_error(identifier, "evaluation_internal_error")
        except ResearchConflict:
            return
        except (ValueError, OSError, SQLAlchemyError, TimeoutError):
            logger.error("评估执行未能确认收尾", evaluation_id=identifier)

    async def run_locked(self, identifier: str) -> None:
        """恢复与自动补算沿用原输入、代码、期限及最多两次总尝试。"""
        row = await self.jobs.read(identifier)
        if row["status"] not in {"pending", "running"}:
            return
        configuration = row["payload"]["configuration"]
        if configuration["source_hash"] != LOADED_SOURCE_IDENTITY:
            raise ResearchSourceChanged("评估源码已改变")
        if row["payload"].get("cancel_requested"):
            raise asyncio.CancelledError
        formal = row["payload"]["request"].get("kind") == "formal_selection_observations"
        if configuration.get("files") is None:
            remaining = (datetime.fromisoformat(configuration["deadline"]) - now()).total_seconds()
            async with asyncio.timeout(max(0, remaining)):
                if formal:
                    configuration = await freeze_selection_observation(self.store, self.settings, row)
                else:
                    configuration = await freeze_evaluation(self.store, self.settings, row)
                await self.jobs.configure(identifier, configuration)
        while True:
            row = await self.jobs.read(identifier)
            remaining = (datetime.fromisoformat(configuration["deadline"]) - now()).total_seconds()
            if remaining <= 0:
                await self.finish_error(identifier, "evaluation_timeout")
                return
            if row["payload"].get("attempt_count", 0) >= 2:
                await self.finish_error(identifier, "evaluation_attempts_exhausted")
                return
            row = await self.jobs.start_attempt(identifier)
            if formal:
                files = SelectionObservationFiles.model_validate(row["payload"]["configuration"]["files"])
            else:
                files = EvaluationFiles.model_validate(row["payload"]["configuration"]["files"]).model_copy(
                    update={"timeout_seconds": max(1, int(remaining))}
                )
            try:
                report = await (
                    self.run_observation_worker(identifier, files, remaining)
                    if formal
                    else self.run_worker(identifier, files, remaining)
                )
            except EvaluationExecutionError as error:
                if error.code == "evaluation_process_failed" and row["payload"]["attempt_count"] < 2:
                    continue
                raise
            if (await self.jobs.read(identifier))["payload"].get("cancel_requested"):
                raise asyncio.CancelledError
            if formal:
                await publish_selection_observation(self.store, identifier, report)
            else:
                await publish_evaluation(self.store, identifier, report)
            return

    async def run_worker(self, identifier: str, files: EvaluationFiles, remaining: float) -> EvaluationReport:
        """另一实例写入取消意图时也会停止worker，终态等待真实退出。"""

        async def checkpoint(frame: EvaluationFrame) -> None:
            await self.jobs.progress(identifier, frame)

        task = asyncio.create_task(execute_evaluation(files, timeout=remaining, checkpoint=checkpoint))
        try:
            while not task.done():
                done, _ = await asyncio.wait((task,), timeout=1)
                if not done and (await self.jobs.read(identifier))["payload"].get("cancel_requested"):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise asyncio.CancelledError
            return await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def run_observation_worker(
        self, identifier: str, files: SelectionObservationFiles, remaining: float
    ) -> SelectionObservationReport:
        """正式观测同样轮询持久取消意图，终态等待真实进程退出。"""

        async def checkpoint(frame: SelectionObservationFrame) -> None:
            await self.jobs.observation_progress(identifier, frame)

        task = asyncio.create_task(execute_selection_observation(files, timeout=remaining, checkpoint=checkpoint))
        try:
            while not task.done():
                done, _ = await asyncio.wait((task,), timeout=1)
                if not done and (await self.jobs.read(identifier))["payload"].get("cancel_requested"):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    raise asyncio.CancelledError
            return await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def finish_error(self, identifier: str, code: str) -> None:
        """持久化失败前不宣称任务已终止；原成功结果保持终态。"""
        try:
            row = await self.jobs.read(identifier)
            if row["payload"].get("cancel_requested") and row["status"] in {"pending", "running"}:
                await self.finish_cancel(identifier)
                return
            await self.jobs.transition(identifier, "failed", expected=("pending", "running"), error_code=code)
        except (ValueError, OSError, SQLAlchemyError, TimeoutError):
            logger.error("评估失败未能持久化", evaluation_id=identifier)

    async def finish_cancel(self, identifier: str) -> None:
        """子进程已清理；服务关闭保留可恢复pending，显式取消才记cancelled。"""
        row = await self.jobs.read(identifier)
        requested = row["payload"].get("cancel_requested", False)
        status = "cancelled" if requested else "pending" if self.closing else "failed"
        await self.jobs.transition(
            identifier, status, expected=("pending", "running"), error_code="evaluation_interrupted"
        )

    async def cancel(self, identifier: str) -> None:
        """本实例等候进程回收；其他拥有者通过持久取消意图收尾。"""
        await self.jobs.request_cancel(identifier)
        task = self.tasks.get(identifier)
        if task:
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 20)
        try:
            async with self.store.execution_lock(identifier, table=EVALUATION_JOBS):
                await self.finish_cancel(identifier)
        except ResearchConflict:
            return

    async def retry(self, identifier: str) -> None:
        """沿原身份显式续跑，不刷新源码、行情或期限。"""
        ensure_source_current()
        async with self.admission:
            if self.closing or len(self.tasks) + len(self.peer_tasks) >= 2:
                raise ResearchConflict("研究任务容量已满或正在关闭")
            await self.jobs.retry(identifier, LOADED_SOURCE_IDENTITY)
            self.start(identifier)

    async def restore(self) -> None:
        """后台只协调有界原任务，GET查询不会顺带启动计算。"""
        await self.scan()
        self.watcher = asyncio.create_task(self.watch())

    async def scan(self) -> None:
        """容量空出后仅恢复数据库中的一条原评估。"""
        if self.closing or len(self.tasks) + len(self.peer_tasks) >= 2:
            return
        async with self.store.engine.connect() as connection:
            identifiers = (
                await connection.scalars(
                    select(EVALUATION_JOBS.c.uuid)
                    .where(
                        EVALUATION_JOBS.c.workspace == self.store.workspace,
                        EVALUATION_JOBS.c.is_deleted.is_(False),
                        EVALUATION_JOBS.c.status.in_(("pending", "running")),
                    )
                    .order_by(EVALUATION_JOBS.c.create_time, EVALUATION_JOBS.c.uuid)
                    .limit(1)
                )
            ).all()
        for identifier in identifiers:
            self.start(identifier)

    async def watch(self) -> None:
        """有界恢复轮询不承担行情或量化计算。"""
        while not self.closing:
            await asyncio.sleep(1)
            try:
                async with self.admission:
                    await self.scan()
            except (ValueError, ResearchUnavailable, OSError, SQLAlchemyError, TimeoutError):
                logger.error("评估恢复扫描未完成")

    async def close(self) -> None:
        """服务关闭等候所属进程真实停止，原任务保留恢复状态。"""
        self.closing = True
        if self.watcher:
            self.watcher.cancel()
            await asyncio.gather(self.watcher, return_exceptions=True)
        tasks = tuple(self.tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 20)
