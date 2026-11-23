"""量化尝试读取当前run历史；与某旧报告冻结状态分别表达。"""

from sqlalchemy import func, select

from stock_research.errors import ResearchIntegrityError
from stock_research.quant_store import job_binding
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import QUANT_ATTEMPTS, QUANT_JOBS, RESEARCH_RUNS
from stock_research.schemas.quant_jobs import QuantAttemptPage, QuantAttemptView


def attempt_view(row: dict[str, object], run: dict[str, object]) -> QuantAttemptView:
    """每次尝试有独立摘要/状态，不能用当前成功job替换旧失败。"""
    body = verified_payload(row)
    try:
        if any(body[key] != value for key, value in job_binding(run["uuid"], row["strategy"], run["payload"]).items()):
            raise ValueError("量化尝试绑定不一致")
        view = QuantAttemptView(
            id=row["uuid"],
            job_id=row["job_uuid"],
            **{
                key: body[key]
                for key in ("strategy", "status", "attempt_count", "artifact_id", "analysis_hash", "error_code")
            },
            create_time=row["create_time"],
            update_time=row["update_time"],
        )
        if (view.status, view.attempt_count) != (row["status"], row["attempt"]):
            raise ValueError("量化尝试索引不一致")
        return view
    except (ValueError, KeyError):
        raise ResearchIntegrityError("量化尝试回执无效") from None


async def quant_attempt_page(store: RunStore, run_id: str, page: int, page_size: int) -> QuantAttemptPage:
    """数据库执行稳定分页，legacy无记录不等于零次失败。"""
    run = await store.row(RESEARCH_RUNS, run_id)
    join = QUANT_ATTEMPTS.join(QUANT_JOBS, QUANT_ATTEMPTS.c.job_uuid == QUANT_JOBS.c.uuid)
    predicate = (
        QUANT_ATTEMPTS.c.workspace == store.workspace,
        QUANT_JOBS.c.workspace == store.workspace,
        QUANT_JOBS.c.run_uuid == run_id,
        QUANT_ATTEMPTS.c.is_deleted.is_(False),
        QUANT_JOBS.c.is_deleted.is_(False),
    )
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(join).where(*predicate))
        rows = (
            (
                await connection.execute(
                    select(QUANT_ATTEMPTS, QUANT_JOBS.c.strategy)
                    .select_from(join)
                    .where(*predicate)
                    .order_by(QUANT_ATTEMPTS.c.create_time.desc(), QUANT_ATTEMPTS.c.uuid.desc())
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    return QuantAttemptPage(
        run_id=run_id,
        recorded=bool(run["payload"].get("native_quant")),
        items=tuple(attempt_view(dict(row), run) for row in rows),
        total=total,
        page=page,
        page_size=page_size,
    )
