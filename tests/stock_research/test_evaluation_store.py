"""评估作业使用原生PostgreSQL验证身份、容量和状态持久化。"""

import asyncio
from datetime import date

import pytest

from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.evaluation import EvaluationReport
from stock_research.evaluation_local import evaluate_files
from stock_research.evaluation_results import evaluation_result, publish_evaluation, sample_page
from stock_research.evaluation_store import EvaluationStore
from stock_research.identity import new_id
from stock_research.run_store import RunStore
from stock_research.schemas.evaluation_execution import EvaluationFiles
from stock_research.schemas.evaluations import CreateEvaluation
from stock_research.store import close_engine, database_engine, initialize_schema


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_idempotency_capacity_and_workspace(postgres_settings: Settings) -> None:
    """[INTEGRATION] 相同意图重放原作业，其他意图容量及跨workspace分别拒绝。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            store = RunStore(engine, "evaluations-" + new_id())
            jobs = EvaluationStore(store)
            body = CreateEvaluation(profile="baseline-v1", cutoff=date(2026, 11, 20))
            first = await jobs.create("same-request-key-0001", body, {"source_hash": "a" * 64})

            repeated = await jobs.create("same-request-key-0001", body, {"source_hash": "b" * 64})

            assert first["uuid"] == repeated["uuid"]
            assert repeated["payload"]["configuration"]["source_hash"] == "a" * 64
            with pytest.raises(ResearchConflict):
                await jobs.create("same-request-key-0001", body.model_copy(update={"profile": "other"}), {})
            with pytest.raises(ResearchConflict):
                await jobs.create("different-key-000001", body, {})
            with pytest.raises(ResearchNotFound):
                await EvaluationStore(RunStore(engine, "another-" + new_id())).read(first["uuid"])
            page = await jobs.page(1, 5)
            assert page.total == 1 and str(page.items[0].id) == first["uuid"]
            assert "configuration" not in page.items[0].model_dump()
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_result_published_atomically_and_paged(
    postgres_settings: Settings, evaluation_files: EvaluationFiles
) -> None:
    """[INTEGRATION] 结果和逐股观测一次提交，重放不重复，跨workspace不泄露。"""
    from pathlib import Path

    rules = {name: rule.model_copy(update={"enabled": False}) for name, rule in evaluation_files.martin.rules.items()}
    evaluation_files = evaluation_files.model_copy(
        update={"martin": evaluation_files.martin.model_copy(update={"rules": rules})}
    )
    output = evaluate_files(evaluation_files)
    report = EvaluationReport.model_validate_json(Path(output["path"]).read_text())

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            store = RunStore(engine, "evaluation-result-" + new_id())
            jobs = EvaluationStore(store)
            configuration = {
                "files": evaluation_files.model_copy(update={"expected_input_hash": report.input_hash}).model_dump(
                    mode="json"
                )
            }
            row = await jobs.create(
                "result-request-000001", CreateEvaluation(profile="baseline-v1", cutoff=report.cutoff), configuration
            )
            await jobs.transition(row["uuid"], "running", expected=("pending",))

            identifier = await publish_evaluation(store, row["uuid"], report)
            repeated = await publish_evaluation(store, row["uuid"], report)

            assert identifier == repeated
            assert (await jobs.read(row["uuid"]))["status"] == "completed"
            result = await evaluation_result(store, identifier)
            assert result.report_hash == report.content_hash and "outcomes" not in result.model_dump()
            samples = await sample_page(store, identifier, 1, 5)
            assert samples.total == sum(len(item.pool) for item in report.sections)
            assert len(samples.items) == 5 and samples.total >= 20
            final = await sample_page(store, identifier, 4, 5, "martin")
            assert sum(item.status == "sealed" and item.windows is None for item in final.items) == 4
            with pytest.raises(ResearchNotFound):
                await evaluation_result(RunStore(engine, "other-" + new_id()), identifier)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_terminal_state_survives_reconnect(postgres_settings: Settings) -> None:
    """[INTEGRATION] 取消后晚到的成功进度不能把原任务重新标成运行中。"""

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        workspace = "evaluations-" + new_id()
        try:
            await initialize_schema(engine)
            jobs = EvaluationStore(RunStore(engine, workspace))
            body = CreateEvaluation(profile="baseline-v1", cutoff=date(2026, 11, 20))
            row = await jobs.create("cancel-request-000001", body, {})
            assert await jobs.transition(row["uuid"], "running", expected=("pending",))
            assert await jobs.transition(row["uuid"], "cancelled", expected=("running",))
            assert not await jobs.transition(row["uuid"], "running", expected=("pending",))
        finally:
            await close_engine(engine)
        reader = database_engine(postgres_settings, test=True)
        try:
            jobs = EvaluationStore(RunStore(reader, workspace))
            assert (await jobs.read(row["uuid"]))["status"] == "cancelled"
            next_row = await jobs.create("next-request-0000001", body, {})
            assert next_row["uuid"] != row["uuid"]
            page = await jobs.page(2, 1)
            assert page.total == 2 and str(page.items[0].id) == row["uuid"]
        finally:
            await close_engine(reader)

    asyncio.run(scenario())
