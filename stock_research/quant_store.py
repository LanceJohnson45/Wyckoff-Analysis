"""按策略保存量化尝试，成功产物只追加；原运行输入清单不修改。"""

import asyncio
from typing import TYPE_CHECKING

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from stock_research.errors import ResearchConflict, ResearchIntegrityError
from stock_research.identity import digest, new_id
from stock_research.quant_compute import QuantReport
from stock_research.schema import QUANT_ATTEMPTS, QUANT_JOBS
from stock_research.schemas.api import QuantJobView

if TYPE_CHECKING:
    from stock_research.run_store import RunStore


def job_binding(run_id: str, strategy: str, manifest: dict[str, object]) -> dict[str, object]:
    """每次恢复都重建绑定，不能把另一日/另一参数的成功任务拼入运行。"""
    return {
        "run_id": run_id,
        "strategy": strategy,
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_hash": manifest["snapshot_hash"],
        "source_hash": manifest["source_hash"],
        "parameter_hash": digest(manifest["parameters"][strategy]),
        "engine_hash": manifest["algorithms"]["wyckoff"] if strategy == "wyckoff" else None,
    }


async def seed_jobs(
    connection: AsyncConnection, workspace: str, run_id: str, manifest: dict[str, object]
) -> tuple[QuantJobView, ...]:
    """任务与首次运行同事务创建；硬中断不留下缺策略的运行。"""
    views = []
    for strategy in ("martin", "wyckoff"):
        identifier = new_id()
        body = {
            **job_binding(run_id, strategy, manifest),
            "status": "pending",
            "attempt_count": 0,
            "artifact_id": None,
            "analysis_hash": None,
            "error_code": None,
        }
        await connection.execute(
            insert(QUANT_JOBS).values(
                uuid=identifier,
                workspace=workspace,
                run_uuid=run_id,
                strategy=strategy,
                status="pending",
                attempt_count=0,
                factor_uuid=None,
                payload=body,
                content_hash=digest(body),
            )
        )
        views.append(
            QuantJobView(
                id=identifier,
                strategy=strategy,
                status="pending",
                attempt_count=0,
                artifact_id=None,
                analysis_hash=None,
                error_code=None,
            )
        )
    return tuple(views)


def job_view(row: dict[str, object], run: dict[str, object]) -> QuantJobView:
    """索引、payload和冻结输入共同校验，损坏记录不是pending或空池。"""
    from stock_research.run_store import verified_payload

    body = verified_payload(row)
    if any(body.get(key) != value for key, value in job_binding(run["uuid"], row["strategy"], run["payload"]).items()):
        raise ResearchIntegrityError("量化任务与冻结输入不一致")
    try:
        view = QuantJobView(
            id=row["uuid"],
            **{
                key: body[key]
                for key in ("strategy", "status", "attempt_count", "artifact_id", "analysis_hash", "error_code")
            },
        )
        if (view.status, view.attempt_count, str(view.artifact_id) if view.artifact_id else None) != (
            row["status"],
            row["attempt_count"],
            row["factor_uuid"],
        ):
            raise ValueError("量化索引不一致")
        if (view.status == "completed") != (view.artifact_id is not None and view.analysis_hash is not None):
            raise ValueError("量化成功绑定不一致")
        return view
    except (ValueError, KeyError):
        raise ResearchIntegrityError("量化任务内容无效") from None


async def job_rows(store: "RunStore", run: dict[str, object]) -> list[dict[str, object]]:
    join = QUANT_JOBS.outerjoin(
        QUANT_ATTEMPTS,
        (QUANT_ATTEMPTS.c.job_uuid == QUANT_JOBS.c.uuid)
        & (QUANT_ATTEMPTS.c.attempt == QUANT_JOBS.c.attempt_count)
        & (QUANT_ATTEMPTS.c.workspace == store.workspace)
        & QUANT_ATTEMPTS.c.is_deleted.is_(False),
    )
    async with store.engine.connect() as connection:
        rows = (
            (
                await connection.execute(
                    select(
                        QUANT_JOBS,
                        QUANT_ATTEMPTS.c.payload.label("attempt_payload"),
                        QUANT_ATTEMPTS.c.content_hash.label("attempt_hash"),
                        QUANT_ATTEMPTS.c.status.label("attempt_status"),
                    )
                    .select_from(join)
                    .where(
                        QUANT_JOBS.c.workspace == store.workspace,
                        QUANT_JOBS.c.run_uuid == run["uuid"],
                        QUANT_JOBS.c.is_deleted.is_(False),
                    )
                    .order_by(QUANT_JOBS.c.strategy)
                )
            )
            .mappings()
            .all()
        )
    result = [dict(row) for row in rows]
    if {row["strategy"] for row in result} != {"martin", "wyckoff"} or len(result) != 2:
        raise ResearchIntegrityError("量化任务缺失或重复")
    for row in result:
        job_view(row, run)
        if row["attempt_count"] and (
            row["attempt_payload"] != row["payload"]
            or row["attempt_hash"] != digest(row["attempt_payload"])
            or row["attempt_status"] != row["status"]
        ):
            raise ResearchIntegrityError("量化任务与本次尝试回执不一致")
    return result


async def projected_manifest(store: "RunStore", run: dict[str, object]) -> dict[str, object]:
    """只投影已经提交的成功绑定；publish会冻结此刻视图到独立修订。"""
    views = [job_view(row, run) for row in await job_rows(store, run)]
    return {
        **run["payload"],
        "quant": [view.model_dump(mode="json") for view in views],
        "factor_results": {view.strategy: str(view.artifact_id) for view in views if view.status == "completed"},
        "analysis_hashes": {view.strategy: view.analysis_hash for view in views if view.status == "completed"},
    }


async def start_job(store: "RunStore", row: dict[str, object]) -> dict[str, object]:
    """先保存running尝试，再启动worker；终态CAS不能覆盖成功任务。"""
    if row["status"] not in {"pending", "failed", "cancelled"}:
        raise ResearchConflict("量化任务已经成功或正在执行")
    body = {**row["payload"], "status": "running", "attempt_count": row["attempt_count"] + 1, "error_code": None}
    async with store.engine.begin() as connection:
        changed = await connection.execute(
            update(QUANT_JOBS)
            .where(
                QUANT_JOBS.c.uuid == row["uuid"],
                QUANT_JOBS.c.workspace == store.workspace,
                QUANT_JOBS.c.content_hash == row["content_hash"],
                QUANT_JOBS.c.status == row["status"],
            )
            .values(status="running", attempt_count=body["attempt_count"], payload=body, content_hash=digest(body))
        )
        if changed.rowcount != 1:
            raise ResearchConflict("量化任务已被其他执行者推进")
        await connection.execute(
            insert(QUANT_ATTEMPTS).values(
                uuid=new_id(),
                workspace=store.workspace,
                job_uuid=row["uuid"],
                attempt=body["attempt_count"],
                status="running",
                payload=body,
                content_hash=digest(body),
            )
        )
    return {
        **row,
        "status": "running",
        "attempt_count": body["attempt_count"],
        "payload": body,
        "content_hash": digest(body),
    }


async def finish_job(
    store: "RunStore",
    run: dict[str, object],
    row: dict[str, object],
    *,
    report: QuantReport | None = None,
    error: str | None = None,
    cancelled: bool = False,
) -> None:
    """成功因子、尝试终态、任务绑定同事务；失败没有空产物。"""
    from stock_research.run_store import register_analyses

    if row["status"] != "running" or (report is None) == (error is None):
        raise ResearchIntegrityError("量化结束状态无效")
    if report is not None:
        report = await asyncio.to_thread(type(report).model_validate_json, report.model_dump_json())
    if report is not None and (
        report.snapshot_id != row["payload"]["snapshot_id"]
        or report.snapshot_hash != row["payload"]["snapshot_hash"]
        or report.parameter_hash != row["payload"]["parameter_hash"]
        or (row["strategy"] == "wyckoff" and report.engine_hash != row["payload"]["engine_hash"])
    ):
        raise ResearchIntegrityError("量化成功产物不属于冻结任务")
    async with store.engine.begin() as connection:
        factors = (
            await register_analyses(
                connection,
                store.workspace,
                run["snapshot_uuid"],
                {row["strategy"]: report.model_dump(mode="json")},
                run["payload"]["source_hash"],
                run["payload"]["prompt_hash"],
            )
            if report
            else {}
        )
        body = {
            **row["payload"],
            "status": "completed" if report else "cancelled" if cancelled else "failed",
            "artifact_id": factors.get(row["strategy"]),
            "analysis_hash": report.content_hash if report else None,
            "error_code": error,
        }
        changed = await connection.execute(
            update(QUANT_JOBS)
            .where(
                QUANT_JOBS.c.uuid == row["uuid"],
                QUANT_JOBS.c.workspace == store.workspace,
                QUANT_JOBS.c.content_hash == row["content_hash"],
                QUANT_JOBS.c.status == "running",
            )
            .values(status=body["status"], factor_uuid=body["artifact_id"], payload=body, content_hash=digest(body))
        )
        if changed.rowcount != 1:
            raise ResearchConflict("量化任务已经结束或失去领取权")
        changed = await connection.execute(
            update(QUANT_ATTEMPTS)
            .where(
                QUANT_ATTEMPTS.c.workspace == store.workspace,
                QUANT_ATTEMPTS.c.job_uuid == row["uuid"],
                QUANT_ATTEMPTS.c.attempt == row["attempt_count"],
                QUANT_ATTEMPTS.c.status == "running",
            )
            .values(status=body["status"], payload=body, content_hash=digest(body))
        )
        if changed.rowcount != 1:
            raise ResearchIntegrityError("量化尝试记录缺失或已经结束")


def quant_state(manifest: dict[str, object], strategy: str) -> QuantJobView:
    """固定报告只解释自己的任务快照，legacy成功不补造尝试次数。"""
    raw = next((row for row in manifest.get("quant", []) if row["strategy"] == strategy), None)
    if raw is not None:
        try:
            return QuantJobView.model_validate(raw)
        except ValueError:
            raise ResearchIntegrityError("冻结量化回执无效") from None
    artifact = manifest["factor_results"].get(strategy)
    if artifact is None:
        raise ResearchIntegrityError("冻结量化回执缺失")
    return QuantJobView(
        id=None,
        strategy=strategy,
        status="completed",
        attempt_count=None,
        artifact_id=artifact,
        analysis_hash=manifest["analysis_hashes"][strategy],
        error_code=None,
    )
