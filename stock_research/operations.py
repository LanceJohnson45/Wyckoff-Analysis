"""工作台操作状态：小型研究投影与固定Personal账本读取。"""

import asyncio
from datetime import date
from time import monotonic

from sqlalchemy import select

from stock_research.config import Settings
from stock_research.errors import ResearchUnavailable
from stock_research.identity import now
from stock_research.mvp_delivery import call_worker
from stock_research.mvp_settings import load_mvp_settings
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.operations import CompletedResearch, DeliveryStatus, OperationsView


class Operations:
    """一个进程最多一个状态子进程及一个目标日缓存，关闭时回收所属读取。"""

    def __init__(self, store: RunStore, settings: Settings) -> None:
        self.store, self.settings = store, settings
        self.lock = asyncio.Lock()
        self.readers: set[asyncio.Task] = set()
        self.target: date | None = None
        self.result: DeliveryStatus | None = None
        self.expires = 0.0
        self.closing = False

    async def view(self, target: date) -> OperationsView:
        """失败不伪造空账本；最新完整研究与目标日通知分别保留。"""
        if self.closing:
            raise ResearchUnavailable("操作状态服务正在关闭")
        delivery = await self.delivery(target)
        statement = (
            select(RESEARCH_RUNS)
            .where(
                RESEARCH_RUNS.c.workspace == self.store.workspace,
                RESEARCH_RUNS.c.is_deleted.is_(False),
                RESEARCH_RUNS.c.status == "completed",
            )
            .order_by(RESEARCH_RUNS.c.update_time.desc(), RESEARCH_RUNS.c.uuid.desc())
            .limit(1)
        )
        async with self.store.engine.connect() as connection:
            row = (await connection.execute(statement)).mappings().first()
        latest = None
        if row:
            manifest = verified_payload(dict(row))
            latest = CompletedResearch(
                run_id=row["uuid"], target_session=manifest["target_session"], update_time=row["update_time"]
            )
        return OperationsView(target_session=target, checked_at=now(), latest_completed=latest, delivery=delivery)

    async def delivery(self, target: date) -> DeliveryStatus | None:
        """限时锁内合并读取；上游失败短暂合并为明确503，不返回过期成功。"""
        if self.settings.mvp_status_config_path is None:
            return None
        try:
            async with asyncio.timeout(15), self.lock:
                if self.closing:
                    raise ResearchUnavailable("操作状态服务正在关闭")
                if target != self.target or monotonic() >= self.expires:
                    owner = asyncio.current_task()
                    self.readers.add(owner)
                    try:
                        config = await asyncio.to_thread(load_mvp_settings, self.settings.mvp_status_config_path)
                        if not self.settings.premarket_source_workspace:
                            raise ValueError("缺少固定Personal工作区")
                        raw = await call_worker(
                            config,
                            "status",
                            {"workspace": self.settings.premarket_source_workspace, "target_session": str(target)},
                            8,
                        )
                        result = DeliveryStatus.model_validate(raw)
                        if result.target_session != target:
                            raise ValueError("账本目标日期不符")
                        self.target, self.result, self.expires = target, result, monotonic() + 10
                    except (OSError, ValueError, TimeoutError):
                        self.target, self.result, self.expires = target, None, monotonic() + 5
                    finally:
                        self.readers.discard(owner)
                if self.result is None:
                    raise ResearchUnavailable("通知账本暂不可读取，请稍后核对")
                return self.result
        except TimeoutError:
            raise ResearchUnavailable("通知账本读取超时，请稍后核对") from None

    async def close(self) -> None:
        """取消实际读取拥有者，等待其finally回收子进程后再关闭数据库。"""
        self.closing = True
        readers = tuple(self.readers)
        for reader in readers:
            reader.cancel()
        await asyncio.wait_for(asyncio.gather(*readers, return_exceptions=True), 7)
