"""私有研究问答任务生命周期，最多两次执行，重启不重放未知模型请求。"""

import asyncio
from datetime import timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from stock_research.chat_runtime import chat_prompt_identity, execute_chat
from stock_research.chat_store import ChatStore
from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchSourceChanged, ResearchUnavailable
from stock_research.identity import SHANGHAI, digest
from stock_research.model_connection import primary_model
from stock_research.observations import frozen_scope
from stock_research.run_store import RunStore
from stock_research.schema import CHAT_TURNS
from stock_research.schemas.chat import ChatContext, ChatTurnView, CreateChatTurn
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current

logger = structlog.get_logger(__name__)


class ChatManager:
    """任务只拥有自己的进程，取消/关闭不接管其他执行者或改写终态。"""

    def __init__(
        self,
        store: RunStore,
        settings: Settings,
        *,
        admission: asyncio.Lock | None = None,
        peer_tasks: dict[str, asyncio.Task[None]] | None = None,
    ) -> None:
        self.store, self.settings = store, settings
        self.chats = ChatStore(store)
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.admission = admission or asyncio.Lock()
        self.peer_tasks = peer_tasks if peer_tasks is not None else {}
        self.closing = False

    async def submit(self, key: str, request: CreateChatTurn) -> ChatTurnView:
        """受理只表示问答已落库；未知网络结果用同一键查回，不重复模型消费。"""
        ensure_source_current()
        if self.closing or not self.settings.api_read_token:
            raise ResearchUnavailable("研究问答未配置或正在关闭")
        async with self.admission:
            existing = await self.chats.existing(key, request)
            if existing:
                if (
                    existing.status == "pending"
                    and str(existing.id) not in self.tasks
                    and len(self.tasks) + len(self.peer_tasks) < 2
                ):
                    self.start(existing)
                return existing
            if len(self.tasks) + len(self.peer_tasks) >= 2:
                raise ResearchConflict("研究问答运行容量已满")
            turn = await self.chats.create(key, request)
            self.start(turn)
            return turn

    def start(self, turn: ChatTurnView) -> None:
        """只启动未开始的输入；DB锁保护重放和另一进程的真实拥有者。"""
        identifier = str(turn.id)
        if identifier in self.tasks:
            return
        ensure_source_current()
        if self.closing:
            raise ResearchUnavailable("研究问答服务正在关闭")
        if len(self.tasks) + len(self.peer_tasks) >= 2:
            raise ResearchConflict("研究任务运行容量已满")
        task = asyncio.create_task(self.run(identifier))
        self.tasks[identifier] = task
        task.add_done_callback(lambda done: self.tasks.pop(identifier, None))

    async def context(self, turn: ChatTurnView) -> ChatContext:
        """上下文从已保存父链和固定报告重建，浏览器不能提供伪造证据。"""
        scope = await frozen_scope(self.store, None, str(turn.report_id))
        body = {
            "workspace": self.store.workspace,
            "run_id": str(scope.view.run_id),
            "report_id": str(turn.report_id),
            "stage_id": str(turn.id),
            "snapshot_id": scope.snapshot.id,
            "snapshot_hash": scope.snapshot.content_hash,
            "as_of": scope.snapshot.as_of.isoformat(),
            "target_session": scope.view.target_session.isoformat(),
            "question": turn.question,
            "history": [item.model_dump(mode="json") for item in await self.chats.history(turn)],
            "deadline": (turn.create_time + timedelta(seconds=self.settings.stage_timeout_seconds))
            .astimezone(SHANGHAI)
            .isoformat(),
            "tool_budget": 20,
            "api_port": self.settings.api_port,
        }
        return ChatContext(**body, content_hash=digest(body))

    async def run(self, identifier: str) -> None:
        """完整执行及结果同一拥有者串行确认；异常不反射问题或连接信息。"""
        try:
            async with self.store.execution_lock(identifier, table=CHAT_TURNS):
                turn = await self.chats.read(identifier)
                if turn.status not in {"pending", "running"}:
                    return
                if turn.status == "running":
                    await self.chats.transition(identifier, "failed", error="execution_interrupted")
                    return
                if turn.source_hash != LOADED_SOURCE_IDENTITY or turn.prompt_hash != chat_prompt_identity():
                    await self.chats.transition(identifier, "failed", error="execution_version_changed")
                    return
                async with asyncio.timeout(self.settings.stage_timeout_seconds):
                    connection = await asyncio.to_thread(primary_model, self.settings)
                    await self.chats.transition(identifier, "running", model=connection.model)
                    result = await execute_chat(self.settings, await self.context(turn), connection)
                    await self.chats.transition(identifier, result.status, result=result)
        except ResearchConflict:
            return
        except asyncio.CancelledError:
            await self.finish_error(identifier, "cancelled", "cancelled")
            raise
        except TimeoutError:
            await self.finish_error(identifier, "failed", "deadline_exceeded")
        except Exception as error:
            logger.error("研究问答执行失败", turn_id=identifier, error_type=type(error).__name__)
            await self.finish_error(
                identifier,
                "failed",
                "execution_version_changed" if isinstance(error, ResearchSourceChanged) else "chat_execution_failed",
            )

    async def finish_error(self, identifier: str, status: str, error: str) -> None:
        """失败也等待持久化；无法确认时只记录固定运维错误，不声称已完成。"""
        try:
            turn = await self.chats.read(identifier)
            if turn.status in {"pending", "running"}:
                await self.chats.transition(identifier, status, error=error)
        except (ValueError, SQLAlchemyError, OSError, TimeoutError):
            logger.error("研究问答终态未能持久化", turn_id=identifier)

    async def restore(self) -> None:
        """有活锁则保留其他拥有者；无锁的失联任务明确中断，不自动重收费。"""
        async with self.store.engine.connect() as connection:
            identifiers = (
                await connection.scalars(
                    select(CHAT_TURNS.c.uuid)
                    .where(
                        CHAT_TURNS.c.workspace == self.store.workspace,
                        CHAT_TURNS.c.is_deleted.is_(False),
                        CHAT_TURNS.c.status.in_(("pending", "running")),
                    )
                    .order_by(CHAT_TURNS.c.create_time, CHAT_TURNS.c.uuid)
                )
            ).all()
        for identifier in identifiers:
            try:
                async with self.store.execution_lock(identifier, table=CHAT_TURNS):
                    await self.finish_error(identifier, "failed", "execution_interrupted")
            except ResearchConflict:
                continue

    async def cancel(self, identifier: str) -> ChatTurnView:
        """取消本实例任务并等待整组执行器收尾，别的执行者由数据库锁保护。"""
        turn = await self.chats.read(identifier)
        if turn.status not in {"pending", "running"}:
            raise ResearchConflict("研究问答已结束")
        task = self.tasks.get(identifier)
        if task:
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 20)
        async with self.store.execution_lock(identifier, table=CHAT_TURNS):
            await self.finish_error(identifier, "cancelled", "cancelled")
        return await self.chats.read(identifier)

    async def close(self) -> None:
        """先停止接单，再等待所拥有任务取消及回执确认。"""
        self.closing = True
        tasks = list(self.tasks.items())
        for _identifier, task in tasks:
            task.cancel()
        await asyncio.wait_for(asyncio.gather(*(task for _identifier, task in tasks), return_exceptions=True), 20)
        for identifier, _task in tasks:
            await self.finish_error(identifier, "cancelled", "api_shutdown")
