"""研究输入准备持久化；硬中断保留未确认，终态不覆盖。"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from sqlalchemy import func, insert, select, update

from stock_research.calendar import session_times
from stock_research.errors import (
    ResearchConflict,
    ResearchInputError,
    ResearchIntegrityError,
    ResearchQualityError,
    ResearchSourceChanged,
)
from stock_research.identity import digest, new_id
from stock_research.models import QualityReport
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import API_REQUESTS, PREPARATION_ATTEMPTS, RESEARCH_RUNS
from stock_research.schemas.market_refresh import RefreshReceipt
from stock_research.schemas.preparations import PreparationPage, PreparationPhase, PreparationState, PreparationView
from stock_research.source_guard import LOADED_SOURCE_IDENTITY

PREPARATION_POLL_SECONDS = 0.05


@asynccontextmanager
async def premarket_preparation_lock(
    store: RunStore, artifacts: Path, as_of: date, *, enabled: bool
) -> AsyncIterator[None]:
    """串行准备共享快照；等待受调用方原预算约束，不占用等待线程。"""
    if not enabled:
        yield
        return
    root = await asyncio.to_thread(artifacts.resolve)
    scope = {"premarket_preparation": str(root), "market": "us", "as_of": as_of.isoformat()}
    key = int(digest(scope)[:16], 16) - (1 << 63)
    while True:
        async with store.engine.begin() as connection:
            if await connection.scalar(select(func.pg_try_advisory_xact_lock(key))):
                yield
                return
        await asyncio.sleep(PREPARATION_POLL_SECONDS)


def preparation_view(row: dict[str, object]) -> PreparationView:
    """业务列和摘要均验证，损坏回执不作为正常空列表返回。"""
    payload = verified_payload(row)
    try:
        result = PreparationView(
            **payload, id=row["uuid"], create_time=row["create_time"], update_time=row["update_time"]
        )
        if (
            result.status,
            result.target_session,
            str(result.run_id) if result.run_id else None,
            str(result.request_id) if result.request_id else None,
        ) != (row["status"], row["target_session"], row["run_uuid"], row["request_uuid"]):
            raise ValueError("准备回执索引与内容不一致")
        return result
    except ValueError:
        raise ResearchIntegrityError("准备回执内容不一致") from None


@dataclass
class Preparation:
    """一次准备拥有自己的阶段和终态，不能被重试覆盖。"""

    store: RunStore
    id: str
    state: PreparationState

    async def save(self, **changes: object) -> None:
        """只可推进尚未确认的本次尝试，提交后才更新内存状态。"""
        state = PreparationState.model_validate({**self.state.model_dump(), **changes})
        payload = state.model_dump(mode="json")
        async with asyncio.timeout(5), self.store.engine.begin() as connection:
            result = await connection.execute(
                update(PREPARATION_ATTEMPTS)
                .where(
                    PREPARATION_ATTEMPTS.c.uuid == self.id,
                    PREPARATION_ATTEMPTS.c.workspace == self.store.workspace,
                    PREPARATION_ATTEMPTS.c.status == "unconfirmed",
                    PREPARATION_ATTEMPTS.c.is_deleted.is_(False),
                )
                .values(
                    payload=payload,
                    content_hash=digest(payload),
                    status=state.status,
                    run_uuid=str(state.run_id) if state.run_id else None,
                )
            )
            if result.rowcount != 1:
                raise ResearchConflict("准备回执已经结束，不能改写")
        self.state = state

    async def checkpoint(self, phase: PreparationPhase, quality: QualityReport | None = None) -> None:
        """缺少质量数据时保留未知，不补零或假定通过。"""
        await self.save(phase=phase, quality=quality if quality is not None else self.state.quality)

    async def ready(self, run_id: str) -> None:
        """运行确实属于当前workspace和目标日后才能确认已冻结。"""
        run = await self.store.row(RESEARCH_RUNS, run_id)
        if run["payload"]["target_session"] != self.state.target_session.isoformat():
            raise ResearchIntegrityError("准备回执与运行目标日不一致")
        await self.save(status="ready", phase="freeze", run_id=run_id)

    async def refresh(self, receipt: RefreshReceipt) -> None:
        """更新属于本次准备，后续进度不能替换已经领取的更新身份。"""
        previous = self.state.market_refresh
        if previous and (previous.id != receipt.id or previous.status != "unconfirmed"):
            raise ResearchIntegrityError("行情更新身份已变化或已经结束")
        await self.save(market_refresh=receipt)


@asynccontextmanager
async def record_preparation(
    store: RunStore, target: date, origin: Literal["api", "cli"], request_id: str | None = None
) -> AsyncIterator[Preparation]:
    """先提交未确认记录；异常仅保存固定错误族，不保存内部异常字符串。"""
    try:
        _, _, as_of = session_times(target)
    except ValueError:
        raise ResearchInputError("目标日期不是正式交易日") from None
    if request_id:
        await store.row(API_REQUESTS, request_id)
    state = PreparationState(
        target_session=target,
        as_of=as_of,
        origin=origin,
        request_id=request_id,
        status="unconfirmed",
        phase="parameters",
        quality=None,
        error_code=None,
        run_id=None,
        source_hash=LOADED_SOURCE_IDENTITY,
    )
    attempt = Preparation(store, new_id(), state)
    payload = state.model_dump(mode="json")
    async with store.engine.begin() as connection:
        await connection.execute(
            insert(PREPARATION_ATTEMPTS).values(
                uuid=attempt.id,
                workspace=store.workspace,
                target_session=target,
                status=state.status,
                request_uuid=request_id,
                run_uuid=None,
                payload=payload,
                content_hash=digest(payload),
            )
        )
    try:
        yield attempt
        if attempt.state.status != "ready":
            await attempt.save(status="failed", error_code="incomplete")
            raise ResearchIntegrityError("研究准备未确认冻结结果")
    except BaseException as error:
        if attempt.state.status == "unconfirmed":
            code = (
                "cancelled"
                if isinstance(error, asyncio.CancelledError)
                else "quality_failed"
                if isinstance(error, ResearchQualityError)
                else "source_changed"
                if isinstance(error, ResearchSourceChanged)
                else "input_unavailable"
            )
            await attempt.save(status="cancelled" if code == "cancelled" else "failed", error_code=code)
        raise


async def preparation_page(store: RunStore, page: int, page_size: int, target: date | None = None) -> PreparationPage:
    """分页始终在SQL执行，日期过滤和确定次序不暴露其他workspace。"""
    predicate = [PREPARATION_ATTEMPTS.c.workspace == store.workspace, PREPARATION_ATTEMPTS.c.is_deleted.is_(False)]
    if target is not None:
        predicate.append(PREPARATION_ATTEMPTS.c.target_session == target)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(PREPARATION_ATTEMPTS).where(*predicate))
        rows = (
            (
                await connection.execute(
                    select(PREPARATION_ATTEMPTS)
                    .where(*predicate)
                    .order_by(PREPARATION_ATTEMPTS.c.create_time.desc(), PREPARATION_ATTEMPTS.c.uuid.desc())
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    return PreparationPage(
        items=tuple(preparation_view(dict(row)) for row in rows), total=total, page=page, page_size=page_size
    )
