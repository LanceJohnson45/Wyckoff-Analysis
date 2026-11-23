"""研究HTTP/工具共用的workspace隔离查询及公开字段投影。"""

from datetime import date, datetime

from sqlalchemy import func, select

from stock_research.errors import ResearchNotFound
from stock_research.identity import now
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import RESEARCH_REPORTS, RESEARCH_RUNS
from stock_research.schemas.api import ReportView, RunListItem, RunPage, RunView
from stock_research.store import load_snapshot


async def run_view(store: RunStore, body: dict[str, object], *, observed_at: datetime | None = None) -> RunView:
    """从冻结清单构造业务视图，不向客户端转发内部文件或模型连接。"""
    manifest = body["manifest"]
    snapshot = await load_snapshot(store.engine, store.workspace, manifest["snapshot_id"])
    observed_at = observed_at or now()
    return RunView(
        run_id=body["run_id"],
        market="us",
        target_session=manifest["target_session"],
        as_of=snapshot.as_of,
        quality=snapshot.quality,
        snapshot_id=manifest["snapshot_id"],
        snapshot_hash=manifest["snapshot_hash"],
        report_due=manifest["report_due"],
        observed_at=observed_at,
        late=observed_at > datetime.fromisoformat(manifest["report_due"]),
        preview=manifest["preview"],
        status=body["status"],
        error_code=body["error_code"],
        selections=[
            dict(
                id=item["id"],
                strategy=item["strategy"],
                decision=item["payload"]["decision"],
                source=item["payload"]["source"],
            )
            for item in body["selections"]
        ],
        reviews=[
            dict(
                id=item["id"],
                strategy=item["strategy"],
                decision=item["payload"]["decision"],
                source=item["payload"]["source"],
            )
            for item in body["reviews"]
        ],
        stages=[
            dict(
                id=item["id"],
                strategy=item["strategy"],
                kind=item["kind"],
                status=item["status"],
                attempt_count=item["attempt_count"],
                target_selection_id=item["target_selection_id"],
                context_hash=item["payload"].get("context_hash"),
                error=item["payload"].get("result", {}).get("error"),
                receipt_available=bool(item["payload"].get("result", {}).get("receipt_path")),
            )
            for item in body["stages"]
        ],
    )


async def list_runs(store: RunStore, page: int, page_size: int) -> RunPage:
    """分页在数据库执行，并以UUID稳定打破相同时间的排序。"""
    predicate = (RESEARCH_RUNS.c.workspace == store.workspace, RESEARCH_RUNS.c.is_deleted.is_(False))
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(RESEARCH_RUNS).where(*predicate))
        rows = (
            (
                await connection.execute(
                    select(RESEARCH_RUNS)
                    .where(*predicate)
                    .order_by(RESEARCH_RUNS.c.create_time.desc(), RESEARCH_RUNS.c.uuid.desc())
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    items = []
    for row in rows:
        manifest = verified_payload(dict(row))
        items.append(
            RunListItem(
                run_id=row["uuid"],
                target_session=manifest["target_session"],
                status=row["status"],
                error_code=row["error_code"],
                create_time=row["create_time"],
            )
        )
    return RunPage(items=items, total=total, page=page, page_size=page_size)


async def report_view(store: RunStore, report_id: str) -> ReportView:
    """固定修订重读其当时的初选与互评，不用当前运行状态覆盖历史。"""
    row = await store.row(RESEARCH_REPORTS, report_id)
    return ReportView(
        report_id=row["uuid"],
        revision=row["revision"],
        create_time=row["create_time"],
        content_hash=row["content_hash"],
        run=await run_view(store, row["payload"], observed_at=row["create_time"]),
    )


async def latest_report_id(store: RunStore, target_session: date | None = None) -> str:
    """最新指最近创建的报告，可包含明确的partial/failed状态。"""
    predicate = [
        RESEARCH_REPORTS.c.workspace == store.workspace,
        RESEARCH_REPORTS.c.is_deleted.is_(False),
        RESEARCH_RUNS.c.workspace == store.workspace,
        RESEARCH_RUNS.c.is_deleted.is_(False),
    ]
    if target_session is not None:
        predicate.append(RESEARCH_RUNS.c.payload["target_session"].as_string() == target_session.isoformat())
    async with store.engine.connect() as connection:
        result = await connection.scalar(
            select(RESEARCH_REPORTS.c.uuid)
            .join(RESEARCH_RUNS, RESEARCH_REPORTS.c.run_uuid == RESEARCH_RUNS.c.uuid)
            .where(*predicate)
            .order_by(
                RESEARCH_RUNS.c.create_time.desc(), RESEARCH_RUNS.c.uuid.desc(), RESEARCH_REPORTS.c.revision.desc()
            )
            .limit(1)
        )
    if result is None:
        raise ResearchNotFound("研究报告不存在或不属于当前workspace")
    return result
