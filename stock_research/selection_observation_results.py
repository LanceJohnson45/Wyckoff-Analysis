"""正式初选观测结果发布与SQL分页读取。"""

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from core.trade_friction import slippage_bps_per_side
from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.evaluation_store import locked_evaluation
from stock_research.identity import digest, new_id
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import EVALUATION_JOBS, EVALUATION_RESULTS, EVALUATION_SAMPLES
from stock_research.schemas.api import Strategy
from stock_research.schemas.selection_observation_execution import SelectionObservationFiles
from stock_research.schemas.selection_observation_jobs import (
    SelectionObservationResultView,
    SelectionObservationSamplePage,
    SelectionObservationSampleView,
)
from stock_research.schemas.selection_observations import SelectionObservationReport
from stock_research.selection_scope import selection_scope


async def freeze_selection_observation(
    store: RunStore, settings: Settings, row: dict[str, object]
) -> dict[str, object]:
    """固定报告初选范围和worker输入位置，不把当前研究资料带入旧任务。"""
    payload = row["payload"]
    request = payload["request"]
    config = payload["configuration"]
    scope = await selection_scope(store, request["report_id"])
    root = (settings.artifacts_path / "selection-observation-jobs" / row["uuid"]).resolve()
    files = SelectionObservationFiles(
        scope=scope,
        cutoff=request["cutoff"],
        market_path=settings.local_market_path.resolve(),
        artifacts_path=root / "artifacts",
        frozen_input_path=root / "input.json",
        source_hash=config["source_hash"],
        slippage_bps_per_side=slippage_bps_per_side(),
    )
    return {**config, "files": files.model_dump(mode="json")}


def observation_result(report: SelectionObservationReport, job_id: str) -> dict[str, object]:
    """小结果元数据保留固定身份，逐股窗口另行分页。"""
    return {
        "kind": report.kind,
        "algorithm": report.algorithm,
        "reference": report.reference,
        "scope": report.scope.model_dump(mode="json"),
        "cutoff": report.cutoff,
        "report_hash": report.scope.report_hash,
        "input_hash": report.input_hash,
        "source_hash": report.source_hash,
        "source_identity": report.source_identity,
        "costs": report.costs,
        "selection_count": len(report.scope.selections),
        "observation_count": len(report.samples),
        "missing_strategies": sorted({"martin", "wyckoff"} - {row.strategy for row in report.scope.selections}),
        "manual_review": "pending",
        "limitations": report.limitations,
    }


def observation_samples(report: SelectionObservationReport, result_id: str, workspace: str) -> list[dict[str, object]]:
    """正式样本按参考日落索引，分母不因缺数或空名单缩小。"""
    rows = []
    for sample in report.samples:
        reference = next(row for row in report.scope.selections if row.selection_id == sample.selection_id)
        payload = sample.model_dump(mode="json", exclude={"strategy", "symbol"})
        payload["reference_session"] = reference.reference_session.isoformat()
        rows.append(
            dict(
                uuid=new_id(),
                workspace=workspace,
                result_uuid=result_id,
                as_of=reference.reference_session,
                strategy=sample.strategy,
                symbol=sample.symbol,
                payload=payload,
                content_hash=digest(payload),
            )
        )
    return rows


def verify_observation_binding(payload: dict[str, object], report: SelectionObservationReport) -> None:
    """结果必须对应原冻结报告范围、源码和输入。"""
    files = SelectionObservationFiles.model_validate(payload["configuration"].get("files"))
    if (
        files.scope != report.scope
        or files.cutoff != report.cutoff
        or files.source_hash != report.source_hash
        or files.expected_input_hash != report.input_hash
    ):
        raise ResearchConflict("正式观测结果与冻结输入不一致")


async def publish_selection_observation(store: RunStore, identifier: str, report: SelectionObservationReport) -> str:
    """结果、样本和成功状态同一事务提交，重复完成不追加第二份。"""
    async with store.engine.begin() as connection:
        row = await locked_evaluation(connection, store, identifier)
        payload = row["payload"]
        verify_observation_binding(payload, report)
        if row["status"] == "completed":
            existing = (
                (
                    await connection.execute(
                        select(EVALUATION_RESULTS).where(
                            EVALUATION_RESULTS.c.workspace == store.workspace,
                            EVALUATION_RESULTS.c.uuid == payload["result_id"],
                            EVALUATION_RESULTS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one()
            )
            if verified_payload(dict(existing))["report_hash"] != report.scope.report_hash:
                raise ResearchConflict("完成的观测不能覆盖为其他结果")
            return existing["uuid"]
        if row["status"] != "running" or payload.get("cancel_requested"):
            raise ResearchConflict("正式观测已取消或不在运行状态")
        result_id = new_id()
        body = observation_result(report, identifier)
        payload = SelectionObservationResultView(id=result_id, job_id=identifier, **body).model_dump(
            mode="json", exclude={"id", "job_id"}
        )
        SelectionObservationResultView(id=result_id, job_id=identifier, **body)
        await connection.execute(
            insert(EVALUATION_RESULTS).values(
                uuid=result_id,
                workspace=store.workspace,
                job_uuid=identifier,
                payload=payload,
                content_hash=digest(payload),
            )
        )
        samples = observation_samples(report, result_id, store.workspace)
        if samples:
            await connection.execute(insert(EVALUATION_SAMPLES), samples)
        job_payload = row["payload"]
        payload = {**job_payload, "status": "completed", "result_id": result_id, "error_code": None}
        await connection.execute(
            update(EVALUATION_JOBS)
            .where(EVALUATION_JOBS.c.uuid == identifier, EVALUATION_JOBS.c.workspace == store.workspace)
            .values(status="completed", payload=payload, content_hash=digest(payload))
        )
    return result_id


async def selection_observation_result(store: RunStore, identifier: str) -> SelectionObservationResultView:
    """只读小结果元数据，不读取行情文件或启动worker。"""
    row = await store.row(EVALUATION_RESULTS, identifier)
    if row["payload"].get("kind") != "formal_selection_observations":
        raise ResearchNotFound("正式初选观测不存在")
    return SelectionObservationResultView(id=row["uuid"], job_id=row["job_uuid"], **row["payload"])


async def selection_observation_sample_page(
    store: RunStore, result_id: str, page: int, page_size: int, strategy: Strategy | None = None
) -> SelectionObservationSamplePage:
    """结果身份和类型先校验，计数与分页共用SQL条件。"""
    result = await store.row(EVALUATION_RESULTS, result_id)
    if result["payload"].get("kind") != "formal_selection_observations":
        raise ResearchNotFound("正式初选观测不存在")
    base = select(EVALUATION_SAMPLES).where(
        EVALUATION_SAMPLES.c.workspace == store.workspace,
        EVALUATION_SAMPLES.c.result_uuid == result_id,
        EVALUATION_SAMPLES.c.is_deleted.is_(False),
    )
    if strategy is not None:
        base = base.where(EVALUATION_SAMPLES.c.strategy == strategy)
    async with store.engine.connect() as connection:
        total = await connection.scalar(select(func.count()).select_from(base.subquery()))
        rows = (
            (
                await connection.execute(
                    base.order_by(
                        EVALUATION_SAMPLES.c.as_of,
                        EVALUATION_SAMPLES.c.strategy,
                        EVALUATION_SAMPLES.c.symbol,
                        EVALUATION_SAMPLES.c.uuid,
                    )
                    .limit(page_size)
                    .offset((page - 1) * page_size)
                )
            )
            .mappings()
            .all()
        )
    return SelectionObservationSamplePage(
        items=tuple(
            SelectionObservationSampleView(
                id=row["uuid"],
                result_id=result_id,
                strategy=row["strategy"],
                symbol=row["symbol"],
                **verified_payload(dict(row)),
            )
            for row in rows
        ),
        total=total,
        page=page,
        page_size=page_size,
    )
