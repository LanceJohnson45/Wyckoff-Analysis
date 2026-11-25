"""参数实验结果只追加；同一作业完成重放不得追加第二份。"""

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_store import locked_evaluation
from stock_research.identity import digest, new_id
from stock_research.parameter_experiments import ParameterExperiment
from stock_research.run_store import RunStore, verified_payload
from stock_research.schema import EVALUATION_JOBS, EVALUATION_RESULTS
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.schemas.parameter_experiment_jobs import (
    CreateParameterExperiment,
    ParameterExperimentResultView,
    ParameterExperimentVariant,
)


def parameter_experiment_summary(experiment: ParameterExperiment, report: EvaluationReport) -> dict[str, object]:
    """只投影小结果，不公开内部缓存路径或完整截面证据。"""
    return ParameterExperimentVariant(
        report_hash=report.content_hash,
        input_hash=report.input_hash,
        parameter_hash=report.parameter_hashes["martin"],
        value=experiment.value,
        candidate_count=report.candidate_count,
        usable_sections=report.usable_sections,
        manual_review=report.manual_review,
        limitations=report.limitations,
    ).model_dump(mode="json")


def verify_parameter_experiment_binding(
    request: CreateParameterExperiment,
    files: EvaluationFiles,
    summaries: tuple[dict[str, object], ...],
) -> None:
    """三个变体必须共享同一冻结输入、源码和显式参数身份。"""
    if len(summaries) != 3:
        raise ResearchConflict("参数实验必须包含三个结果")
    variants = [ParameterExperimentVariant.model_validate(summary) for summary in summaries]
    if len({variant.input_hash for variant in variants}) != 1 or variants[0].input_hash != files.expected_input_hash:
        raise ResearchConflict("参数实验结果与冻结输入不一致")
    if any(variant.report_hash == variant.input_hash for variant in variants):
        raise ResearchConflict("参数实验结果摘要无效")
    for variant, value in zip(variants, request.values, strict=True):
        if variant.value != value or variant.parameter_hash not in {
            digest(files.martin.model_copy(update={request.field: value}).model_dump(mode="json")),
            digest(
                files.martin.model_copy(
                    update={
                        request.field: value,
                        "version": f"{files.martin.version}:experiment:{request.field}:{value}",
                    }
                ).model_dump(mode="json")
            ),
        }:
            raise ResearchConflict("参数实验结果与显式参数不一致")


async def publish_parameter_experiment(
    store: RunStore,
    identifier: str,
    request: CreateParameterExperiment,
    summaries: tuple[dict[str, object], ...],
    source_hash: str,
) -> str:
    """结果和成功状态同事务提交；完成重放保持原身份与结果。"""
    files = EvaluationFiles.model_validate(
        (await store.row(EVALUATION_JOBS, identifier))["payload"]["configuration"]["files"]
    )
    verify_parameter_experiment_binding(request, files, summaries)
    async with store.engine.begin() as connection:
        row = await locked_evaluation(connection, store, identifier)
        payload = row["payload"]
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
            if verified_payload(dict(existing))["input_hash"] != files.expected_input_hash:
                raise ResearchConflict("完成的实验不能覆盖为其他结果")
            return existing["uuid"]
        if row["status"] != "running" or payload.get("cancel_requested"):
            raise ResearchConflict("参数实验已取消或不在运行状态")
        result_id = new_id()
        body = dict(
            kind="parameter_experiments",
            profile=request.profile,
            cutoff=request.cutoff.isoformat(),
            field=request.field,
            values=list(request.values),
            input_hash=files.expected_input_hash,
            source_hash=source_hash,
            variants=list(summaries),
            manual_review="pending",
            limitations=("首轮单组描述性实验，封存组不输出表现；不发布生产参数，也不构成调参依据",),
        )
        ParameterExperimentResultView(id=result_id, job_id=identifier, **body)
        await connection.execute(
            insert(EVALUATION_RESULTS).values(
                uuid=result_id,
                workspace=store.workspace,
                job_uuid=identifier,
                payload=body,
                content_hash=digest(body),
            )
        )
        job_payload = {**payload, "status": "completed", "result_id": result_id, "error_code": None}
        await connection.execute(
            update(EVALUATION_JOBS)
            .where(EVALUATION_JOBS.c.uuid == identifier, EVALUATION_JOBS.c.workspace == store.workspace)
            .values(status="completed", payload=job_payload, content_hash=digest(job_payload))
        )
    return result_id


async def parameter_experiment_result(store: RunStore, identifier: str) -> ParameterExperimentResultView:
    """只读小结果元数据，不读取行情或重新量化。"""
    row = await store.row(EVALUATION_RESULTS, identifier)
    if row["payload"].get("kind") != "parameter_experiments":
        raise ResearchNotFound("参数实验不存在")
    return ParameterExperimentResultView(id=row["uuid"], job_id=row["job_uuid"], **row["payload"])
