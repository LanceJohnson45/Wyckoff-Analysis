"""评估结果只追加，逐股窗口单独SQL分页，不读取大行情文件。"""

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from stock_research.errors import ResearchConflict
from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_store import locked_evaluation
from stock_research.identity import digest, new_id
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import EVALUATION_JOBS, EVALUATION_RESULTS, EVALUATION_SAMPLES
from stock_research.schemas.api import Strategy
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.schemas.evaluations import EvaluationResultView, EvaluationSample, EvaluationSamplePage


def evaluation_samples(report: EvaluationReport, result_id: str, workspace: str) -> list[dict[str, object]]:
    """保留每个原池成员，缺数与封存样本均不从分母中移走。"""
    rows = []
    for section in report.sections:
        for symbol in section.pool:
            windows = section.outcomes[symbol] if section.outcomes is not None else None
            statuses = {window.status for window in windows} if windows is not None else {"sealed"}
            status = next(kind for kind in ("sealed", "missing_data", "pending", "completed") if kind in statuses)
            sample = EvaluationSample(
                id=new_id(),
                result_id=result_id,
                as_of=section.section.as_of,
                strategy=section.strategy,
                symbol=symbol,
                partition=section.section.partition,
                purged=section.section.purged,
                status=status,
                windows=windows,
            )
            payload = sample.model_dump(mode="json", exclude={"id", "result_id"})
            rows.append(
                dict(
                    uuid=str(sample.id),
                    workspace=workspace,
                    result_uuid=result_id,
                    as_of=sample.as_of,
                    strategy=sample.strategy,
                    symbol=symbol,
                    payload=payload,
                    content_hash=digest(payload),
                )
            )
    return rows


def verify_evaluation_binding(payload: dict[str, object], report: EvaluationReport) -> None:
    """必须对应原冻结参数、源码与行情，不能提交同日另一配置的合法结果。"""
    files = EvaluationFiles.model_validate(payload["configuration"].get("files"))
    if (
        files.expected_input_hash != report.input_hash
        or files.source_hash != report.source_hash
        or files.cutoff != report.cutoff
    ):
        raise ResearchConflict("评估结果与冻结输入不一致")
    if report.allow_current_universe != files.allow_current_universe or report.parameter_hashes != {
        "martin": digest(files.martin.model_dump(mode="json")),
        "wyckoff": digest(files.wyckoff.model_dump(mode="json")),
    }:
        raise ResearchConflict("评估结果与冻结参数不一致")


async def publish_evaluation(store: RunStore, identifier: str, report: EvaluationReport) -> str:
    """结果、样本与作业成功同一事务提交；未知响应重放不产生第二份。"""
    async with store.engine.begin() as connection:
        row = await locked_evaluation(connection, store, identifier)
        payload = row["payload"]
        verify_evaluation_binding(payload, report)
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
            if verified_payload(dict(existing))["report_hash"] != report.content_hash:
                raise ResearchConflict("完成的评估不能覆盖为其他结果")
            return existing["uuid"]
        if row["status"] != "running" or payload.get("cancel_requested"):
            raise ResearchConflict("评估已取消或不在运行状态")
        result_id = new_id()
        result = report.model_dump(mode="json", exclude={"sections", "schema_version", "content_hash"})
        result.update(
            report_hash=report.content_hash,
            sections=[
                section.model_dump(mode="json", exclude={"outcomes", "analysis_file"}) for section in report.sections
            ],
        )
        EvaluationResultView(id=result_id, job_id=identifier, **result)
        await connection.execute(
            insert(EVALUATION_RESULTS).values(
                uuid=result_id,
                workspace=store.workspace,
                job_uuid=identifier,
                payload=result,
                content_hash=digest(result),
            )
        )
        samples = evaluation_samples(report, result_id, store.workspace)
        if samples:
            await connection.execute(insert(EVALUATION_SAMPLES), samples)
        payload = {
            **payload,
            "status": "completed",
            "completed_sections": 20,
            "result_id": result_id,
            "error_code": None,
        }
        await connection.execute(
            update(EVALUATION_JOBS)
            .where(
                EVALUATION_JOBS.c.uuid == identifier,
                EVALUATION_JOBS.c.workspace == store.workspace,
            )
            .values(status="completed", payload=payload, content_hash=digest(payload))
        )
    return result_id


async def evaluation_result(store: RunStore, identifier: str) -> EvaluationResultView:
    """只读小结果元数据，不到文件系统解析行情或重复量化。"""
    row = await store.row(EVALUATION_RESULTS, identifier)
    return EvaluationResultView(id=row["uuid"], job_id=row["job_uuid"], **row["payload"])


async def sample_page(
    store: RunStore, result_id: str, page: int, page_size: int, strategy: Strategy | None = None
) -> EvaluationSamplePage:
    """结果身份先校验，计数与分页查询共享相同SQL过滤。"""
    await store.row(EVALUATION_RESULTS, result_id)
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
    return EvaluationSamplePage(
        items=tuple(
            EvaluationSample(
                id=row["uuid"],
                result_id=result_id,
                **verified_payload(dict(row)),
            )
            for row in rows
        ),
        total=total,
        page=page,
        page_size=page_size,
    )
