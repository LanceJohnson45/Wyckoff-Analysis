"""API拥有的有限研究任务；冻结先落库，执行、取消与关闭均可追踪。"""

import asyncio
from pathlib import Path

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from stock_research.analysis import MartinReport
from stock_research.calendar import session_times
from stock_research.config import Settings
from stock_research.errors import (
    ResearchConflict,
    ResearchInputError,
    ResearchQualityError,
    ResearchSourceChanged,
    ResearchUnavailable,
)
from stock_research.models import QualityReport, Snapshot
from stock_research.parameter_config import resolve_run_parameters
from stock_research.pipeline import prepare_preview, resume_preview
from stock_research.preparations import record_preparation
from stock_research.request_store import RequestStore
from stock_research.research_commands import analyze_command
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.requests import CreateRun
from stock_research.source_guard import ensure_source_current
from stock_research.wyckoff import WyckoffReport

logger = structlog.get_logger(__name__)


async def prepare_from_market(store: RunStore, settings: Settings, data: CreateRun, request_id: str) -> str:
    """只读operator固定行情路径；请求不接受文件、模型或连接配置。"""
    from stock_research.cli import snapshot_command

    ensure_source_current()
    try:
        _, _, previous = session_times(data.target_session)
    except ValueError:
        raise ResearchInputError("目标日期不是正式交易日") from None
    try:
        async with record_preparation(store, data.target_session, "api", request_id) as attempt, asyncio.timeout(120):
            settings, parameter_sources = await resolve_run_parameters(store, settings)
            await attempt.checkpoint("snapshot")
            output, status = await asyncio.to_thread(snapshot_command, settings, previous, offline=True)
            await attempt.checkpoint("snapshot", QualityReport.model_validate(output["quality"]))
            if status:
                raise ResearchQualityError("行情覆盖或基准未通过质量门槛")
            path = Path(output["path"])
            snapshot = await asyncio.to_thread(lambda: Snapshot.model_validate_json(path.read_text()))
            await attempt.checkpoint("analysis")
            analyses = {}
            for strategy, model in (("martin", MartinReport), ("wyckoff", WyckoffReport)):
                result = await asyncio.to_thread(analyze_command, settings, path, previous, strategy=strategy)
                analyses[strategy] = await asyncio.to_thread(
                    lambda model=model, result=result: model.model_validate_json(Path(result["path"]).read_text())
                )
            ensure_source_current()
            await attempt.checkpoint("freeze")
            run_id, _connections = await prepare_preview(
                store.engine,
                settings,
                snapshot,
                analyses,
                data.target_session,
                parent_id=str(data.parent_run_id) if data.parent_run_id else None,
                force=data.force,
                request_id=request_id,
                parameter_sources=parameter_sources,
            )
            await attempt.ready(run_id)
            return run_id
    except (OSError, SQLAlchemyError, TimeoutError, ValueError):
        raise ResearchUnavailable("无法冻结研究输入，请检查数据与执行配置") from None


class RunManager:
    """一个API实例最多拥有两个任务，进程间最终执行权由数据库锁约束。"""

    def __init__(
        self,
        store: RunStore,
        settings: Settings,
        *,
        admission: asyncio.Lock | None = None,
        peer_tasks: dict[str, asyncio.Task[None]] | None = None,
    ) -> None:
        self.store, self.settings = store, settings
        self.requests = RequestStore(store)
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.closing = False
        self.admission = admission or asyncio.Lock()
        self.peer_tasks = peer_tasks if peer_tasks is not None else {}

    async def submit(self, key: str, data: CreateRun) -> str:
        """相同key重放原运行；更换载荷冲突，冻结和绑定中断均可恢复。"""
        async with self.admission:
            return await self.freeze_request(key, data)

    async def freeze_request(self, key: str, data: CreateRun) -> str:
        """持有接单锁，避免多个请求同时冻结后突破任务容量。"""
        if self.closing:
            raise ResearchUnavailable("研究服务正在关闭")
        ensure_source_current()
        if data.parent_run_id:
            await self.store.row(RESEARCH_RUNS, str(data.parent_run_id))
        async with self.requests.claim(key, data.model_dump(mode="json")) as request:
            run_id = await self.requests.frozen_run(request)
            if run_id is None:
                if len(self.tasks) + len(self.peer_tasks) >= 2:
                    raise ResearchConflict("研究运行容量已满")
                run_id = await prepare_from_market(self.store, self.settings, data, request["uuid"])
            if request["run_uuid"] is None:
                await self.requests.bind(request, run_id)
            run = await self.store.row(RESEARCH_RUNS, run_id)
            if run["status"] in {"pending", "running"}:
                self.start(run_id)
            return run_id

    def start(self, run_id: str) -> None:
        """保留任务强引用；已有任务不重启，完成后移除引用。"""
        if run_id in self.tasks:
            return
        ensure_source_current()
        if self.closing or len(self.tasks) + len(self.peer_tasks) >= 2:
            raise ResearchConflict("研究任务正在关闭或容量已满")
        task = asyncio.create_task(self.run(run_id))
        self.tasks[run_id] = task
        task.add_done_callback(lambda done: self.tasks.pop(run_id, None))

    async def run(self, run_id: str) -> None:
        """无法加载执行配置也记录失败，不能留在pending伪装仍在运行。"""
        try:
            await resume_preview(self.store.engine, self.settings, run_id)
        except ResearchConflict:
            return
        except Exception as error:
            logger.error("研究后台任务失败", run_id=run_id, error_type=type(error).__name__)
            code = (
                "execution_version_changed"
                if isinstance(error, ResearchSourceChanged)
                else "executor_configuration_failed"
            )
            try:
                async with self.store.execution_lock(run_id):
                    body = await self.store.summary(run_id)
                    if body["status"] != "completed":
                        await self.store.recover_stages(run_id, code)
                        await self.store.publish(
                            run_id,
                            "partial" if body["selections"] else "failed",
                            code,
                            track=True,
                        )
            except (OSError, SQLAlchemyError, ResearchConflict):
                logger.error("研究失败状态未能持久化", run_id=run_id)

    async def retry(self, run_id: str) -> None:
        """仅补未成功阶段；已完成运行必须通过force新建版本。"""
        ensure_source_current()
        async with self.admission:
            if self.closing or len(self.tasks) + len(self.peer_tasks) >= 2:
                raise ResearchConflict("研究任务正在关闭或容量已满")
            row = await self.store.row(RESEARCH_RUNS, run_id)
            if row["status"] == "completed" or run_id in self.tasks:
                raise ResearchConflict("当前运行不能重试")
            async with self.store.execution_lock(run_id):
                await self.store.recover_stages(run_id)
            self.start(run_id)

    async def restore(self) -> None:
        """只恢复API创建的未结束任务，不接管CLI独立运行。"""
        async with self.store.engine.connect() as connection:
            rows = (
                (
                    await connection.execute(
                        select(RESEARCH_RUNS.c.uuid)
                        .where(
                            RESEARCH_RUNS.c.workspace == self.store.workspace,
                            RESEARCH_RUNS.c.is_deleted.is_(False),
                            RESEARCH_RUNS.c.status.in_(("pending", "running")),
                            RESEARCH_RUNS.c.payload["api_request_id"].as_string().is_not(None),
                        )
                        .order_by(RESEARCH_RUNS.c.create_time, RESEARCH_RUNS.c.uuid)
                        .limit(2)
                    )
                )
                .scalars()
                .all()
            )
        for run_id in rows:
            self.start(run_id)

    async def cancel(self, run_id: str) -> None:
        """取消本实例拥有的任务并等候子进程收尾，不接管其他执行者。"""
        row = await self.store.row(RESEARCH_RUNS, run_id)
        if row["status"] in {"completed", "partial", "failed", "cancelled"}:
            raise ResearchConflict("运行已结束")
        task = self.tasks.get(run_id)
        if task:
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 20)
        async with self.store.execution_lock(run_id):
            current = await self.store.row(RESEARCH_RUNS, run_id)
            if current["status"] in {"completed", "partial", "failed"}:
                raise ResearchConflict("运行已在取消前结束")
            await self.store.recover_stages(run_id, "cancelled")
            await self.store.publish(run_id, "cancelled", track=True)

    async def close(self) -> None:
        """先停止接单，再取消并收尾任务，最后由应用关闭数据库。"""
        self.closing = True
        owned = list(self.tasks.items())
        for _run_id, task in owned:
            task.cancel()
        await asyncio.wait_for(asyncio.gather(*(task for _, task in owned), return_exceptions=True), 20)
        for run_id, _task in owned:
            row = await self.store.row(RESEARCH_RUNS, run_id)
            if row["status"] in {"pending", "running"}:
                async with self.store.execution_lock(run_id):
                    current = await self.store.row(RESEARCH_RUNS, run_id)
                    if current["status"] in {"pending", "running"}:
                        await self.store.recover_stages(run_id, "api_shutdown")
                        await self.store.publish(run_id, "cancelled", "api_shutdown", track=True)
