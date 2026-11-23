"""人工维护历史与提议的数据库分页，不将私人建议交给研究GET身份。"""

from typing import Literal

from sqlalchemy import exists, func, select

from stock_research.config import Settings
from stock_research.input_maintenance import InputMaintenance
from stock_research.input_records import publication_head, publication_select, publication_view
from stock_research.run_store import RunStore
from stock_research.schema import INPUT_PUBLICATIONS, THEME_PROPOSALS
from stock_research.schemas.input_maintenance import InputPublicationPage, ManualHead, ThemeProposalPage


async def theme_proposals(
    store: RunStore,
    settings: Settings,
    page: int,
    page_size: int,
    status: Literal["pending", "confirmed"] | None = None,
    name: str | None = None,
) -> ThemeProposalPage:
    """确认状态从真实发布关联取得，过滤和稳定分页在PG中执行。"""
    predicate = [THEME_PROPOSALS.c.workspace == store.workspace, THEME_PROPOSALS.c.is_deleted.is_(False)]
    if name is not None:
        predicate.append(THEME_PROPOSALS.c.name == name)
    confirmed = exists(
        select(INPUT_PUBLICATIONS.c.uuid).where(
            INPUT_PUBLICATIONS.c.workspace == store.workspace,
            INPUT_PUBLICATIONS.c.proposal_uuid == THEME_PROPOSALS.c.uuid,
            INPUT_PUBLICATIONS.c.is_deleted.is_(False),
        )
    )
    if status is not None:
        predicate.append(confirmed if status == "confirmed" else ~confirmed)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(THEME_PROPOSALS).where(*predicate))
        identifiers = (
            await connection.scalars(
                select(THEME_PROPOSALS.c.uuid)
                .where(*predicate)
                .order_by(THEME_PROPOSALS.c.create_time.desc(), THEME_PROPOSALS.c.uuid.desc())
                .limit(page_size)
                .offset((page - 1) * page_size)
            )
        ).all()
    maintenance = InputMaintenance(store, settings)
    items = tuple([await maintenance.proposal(identifier) for identifier in identifiers])
    return ThemeProposalPage(items=items, total=total, page=page, page_size=page_size)


async def input_publications(
    store: RunStore,
    page: int,
    page_size: int,
    kind: Literal["theme", "manual"],
    target: str | None = None,
) -> InputPublicationPage:
    """追加历史包含未来生效修订，当前研究是否消费仍由截止日判断。"""
    table = INPUT_PUBLICATIONS
    predicate = [table.c.workspace == store.workspace, table.c.kind == kind, table.c.is_deleted.is_(False)]
    if target is not None:
        predicate.append(table.c.target == target)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(table).where(*predicate))
        rows = (
            (
                await connection.execute(
                    publication_select()
                    .where(*predicate)
                    .order_by(table.c.create_time.desc(), table.c.uuid.desc())
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    return InputPublicationPage(
        items=tuple(publication_view(dict(row)) for row in rows), total=total, page=page, page_size=page_size
    )


async def manual_head(store: RunStore, symbol: str) -> ManualHead:
    """返回该代码最高发布修订，不能以已生效旧修订作为新写入CAS。"""
    async with store.engine.connect() as connection:
        publication = await publication_head(connection, store.workspace, "manual", symbol)
    return ManualHead(symbol=symbol, revision=publication.revision if publication else 0, publication=publication)
