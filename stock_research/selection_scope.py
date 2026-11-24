"""从固定报告核对正式初选，不读取大行情或补入后来完成的名单。"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.calendar import exchange_calendar
from stock_research.decisions import SelectionDecision
from stock_research.errors import ResearchIntegrityError, ResearchNotFound
from stock_research.identity import SHANGHAI
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import RESEARCH_REPORTS, RESEARCH_RUNS, STRATEGY_SELECTIONS
from stock_research.schemas.selection_observations import SelectionBinding, SelectionScope


def forward_reference(target: date, committed_at: datetime) -> date:
    """恰好开盘也不倒填成交参考价，晚到初选顺延至下一正式开盘。"""
    if committed_at.tzinfo is None:
        raise ValueError("初选提交时间须带时区")
    calendar = exchange_calendar()
    if not calendar.is_session(pd.Timestamp(target)):
        raise ValueError("初选目标日须为XNYS交易日")
    day = calendar.date_to_session(
        pd.Timestamp(committed_at.astimezone(ZoneInfo("America/New_York")).date()), direction="next"
    )
    day = max(day, pd.Timestamp(target))
    if calendar.session_open(day) <= pd.Timestamp(committed_at):
        day = calendar.next_session(day)
    return day.date()


async def selection_binding(
    connection: AsyncConnection, store: RunStore, report: dict[str, object], item: dict[str, object]
) -> SelectionBinding:
    """数据库原记录与报告冻结的载荷必须相同，不能信任单独传来的UUID。"""
    row = (
        (
            await connection.execute(
                select(STRATEGY_SELECTIONS).where(
                    STRATEGY_SELECTIONS.c.uuid == item["id"],
                    STRATEGY_SELECTIONS.c.workspace == store.workspace,
                    STRATEGY_SELECTIONS.c.run_uuid == report["run_uuid"],
                    STRATEGY_SELECTIONS.c.is_deleted.is_(False),
                )
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise ResearchIntegrityError("固定报告引用的初选不可读取")
    payload = verified_payload(dict(row))
    decision = SelectionDecision.model_validate(payload["decision"])
    if (
        payload != item["payload"]
        or row["strategy"] != item["strategy"]
        or decision.strategy != row["strategy"]
        or decision.context_hash != payload["context_hash"]
        or row["create_time"] > report["create_time"]
    ):
        raise ResearchIntegrityError("初选与固定报告的身份或时间不一致")
    return SelectionBinding(
        selection_id=row["uuid"],
        strategy=row["strategy"],
        selection_hash=row["content_hash"],
        context_hash=decision.context_hash,
        committed_at=row["create_time"].astimezone(SHANGHAI),
        reference_session=forward_reference(
            date.fromisoformat(report["payload"]["manifest"]["target_session"]), row["create_time"]
        ),
        source=payload["source"],
        symbols=tuple(item.symbol for item in decision.items),
    )


async def selection_scope(store: RunStore, report_id: str) -> SelectionScope:
    """同一只读可重复读事务绑定报告、运行及最多两份不可变初选。"""
    async with store.engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="REPEATABLE READ", postgresql_readonly=True)
        async with connection.begin():
            report = (
                (
                    await connection.execute(
                        select(RESEARCH_REPORTS).where(
                            RESEARCH_REPORTS.c.uuid == report_id,
                            RESEARCH_REPORTS.c.workspace == store.workspace,
                            RESEARCH_REPORTS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .first()
            )
            if report is None:
                raise ResearchNotFound("固定报告不存在")
            body = verified_payload(dict(report))
            run = (
                (
                    await connection.execute(
                        select(RESEARCH_RUNS).where(
                            RESEARCH_RUNS.c.uuid == report["run_uuid"],
                            RESEARCH_RUNS.c.workspace == store.workspace,
                            RESEARCH_RUNS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .first()
            )
            if run is None or body["run_id"] != report["run_uuid"]:
                raise ResearchIntegrityError("固定报告运行身份不一致")
            manifest = body["manifest"]
            current = verified_payload(dict(run))
            if any(manifest[key] != current[key] for key in ("snapshot_id", "snapshot_hash", "target_session")):
                raise ResearchIntegrityError("固定报告快照身份不一致")
            if len(body["selections"]) > 2:
                raise ResearchIntegrityError("固定报告初选数量越界")
            bindings = [await selection_binding(connection, store, dict(report), item) for item in body["selections"]]
    return SelectionScope(
        workspace=store.workspace,
        report_id=report_id,
        report_hash=report["content_hash"],
        report_revision=report["revision"],
        report_status=body["status"],
        run_id=report["run_uuid"],
        snapshot_id=manifest["snapshot_id"],
        snapshot_hash=manifest["snapshot_hash"],
        target_session=manifest["target_session"],
        selections=tuple(bindings),
    )
