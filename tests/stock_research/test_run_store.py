"""真实隔离PostgreSQL验证研究账本、并发领取和不可变交叉评议。"""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from stock_research.analysis import compute_martin
from stock_research.config import Settings
from stock_research.evidence import build_context
from stock_research.identity import digest, new_id, now
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import program_result, run_preview
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_REPORTS, STAGE_RUNS, STRATEGY_SELECTIONS
from stock_research.store import close_engine, database_engine, initialize_schema, save_snapshot
from stock_research.wyckoff import WyckoffParameters, compute_wyckoff

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def analyses_for(snapshot: Snapshot) -> dict[str, object]:
    return {
        "martin": compute_martin(snapshot, MartinParameters()),
        "wyckoff": compute_wyckoff(snapshot, WyckoffParameters()),
    }


async def seed(store: RunStore, snapshot: Snapshot) -> tuple[str, Snapshot, dict[str, object]]:
    await initialize_schema(store.engine)
    saved = await save_snapshot(store.engine, store.workspace, snapshot)
    analyses = analyses_for(saved)
    factors = await store.artifacts(
        saved.id, {k: v.model_dump(mode="json") for k, v in analyses.items()}, "fixture", "fixture"
    )
    manifest = {"snapshot_id": saved.id, "snapshot_hash": saved.content_hash, "factor_results": factors}
    run_id = await store.create_run(manifest)
    return run_id, saved, analyses


def test_stage_commits_once_and_review_targets_only_committed_selection(
    postgres_settings: Settings, snapshot: Snapshot
) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "commit-" + new_id())
        try:
            run_id, saved, analyses = await seed(store, snapshot)
            stage_id = await store.ensure_stage(run_id, "martin", "select")
            context = build_context(
                saved,
                analyses["martin"],
                workspace=store.workspace,
                run_id=run_id,
                stage_id=stage_id,
                role="select",
                deadline=now() + timedelta(seconds=60),
            )
            await store.start_stage(stage_id, context)
            with pytest.raises(ValueError, match="身份冲突"):
                await store.start_stage(stage_id, context)
            selected_id = await store.finish_stage(stage_id, context, program_result(context, None))
            before = await store.row(STRATEGY_SELECTIONS, selected_id)
            with pytest.raises(ValueError, match="身份"):
                await store.finish_stage(stage_id, context, program_result(context, None))
            review_id = await store.ensure_stage(run_id, "wyckoff", "review", selected_id)
            review = build_context(
                saved,
                analyses["wyckoff"],
                workspace=store.workspace,
                run_id=run_id,
                stage_id=review_id,
                role="review",
                targets=(),
                deadline=now() + timedelta(seconds=60),
            )
            await store.start_stage(review_id, review)
            await store.finish_stage(review_id, review, program_result(review, {"selection_id": selected_id}))
            result = await store.summary(run_id)
            assert len(result["selections"]) == len(result["reviews"]) == 1
            assert result["reviews"][0]["payload"]["decision"]["target_selection_id"] == selected_id
            assert await store.row(STRATEGY_SELECTIONS, selected_id) == before
            with pytest.raises(ValueError, match="另一策略"):
                await store.ensure_stage(run_id, "martin", "review", selected_id)
            with pytest.raises(ValueError, match="workspace"):
                await RunStore(engine, "foreign").summary(run_id)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_lock_and_recovery_preserve_committed_results(postgres_settings: Settings, snapshot: Snapshot) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "recovery-" + new_id())
        try:
            run_id, saved, analyses = await seed(store, snapshot)
            async with store.execution_lock(run_id):
                with pytest.raises(ValueError, match="执行者"):
                    async with store.execution_lock(run_id):
                        pytest.fail("second executor acquired an owned lock")
                stage_id = await store.ensure_stage(run_id, "martin", "select")
                context = build_context(
                    saved,
                    analyses["martin"],
                    workspace=store.workspace,
                    run_id=run_id,
                    stage_id=stage_id,
                    role="select",
                    deadline=now() + timedelta(seconds=60),
                )
                await store.start_stage(stage_id, context)
                await store.recover_stages(run_id)
                recovered = await store.row(STAGE_RUNS, stage_id)
                assert recovered["status"] == "failed"
                assert recovered["payload"]["result"]["error"] == "previous_executor_stopped"
                await store.start_stage(stage_id, context)
                await store.finish_stage(stage_id, context, program_result(context, None))
                await store.recover_stages(run_id)
                assert (await store.row(STAGE_RUNS, stage_id))["attempt_count"] == 2
                first = await store.publish(run_id, "partial")
                assert await store.publish(run_id, "partial") == first
                async with engine.connect() as connection:
                    count = await connection.scalar(
                        select(func.count()).select_from(RESEARCH_REPORTS).where(RESEARCH_REPORTS.c.run_uuid == run_id)
                    )
                assert count == 1
            async with store.execution_lock(run_id):
                assert (await store.row(STAGE_RUNS, stage_id))["status"] == "completed"
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_context_self_reported_hash_cannot_expand_authorized_pool(
    postgres_settings: Settings, snapshot: Snapshot
) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "scope-" + new_id())
        try:
            run_id, saved, analyses = await seed(store, snapshot)
            stage_id = await store.ensure_stage(run_id, "martin", "select")
            context = build_context(
                saved,
                analyses["martin"],
                workspace=store.workspace,
                run_id=run_id,
                stage_id=stage_id,
                role="select",
                deadline=now() + timedelta(seconds=60),
            )
            body = context.model_dump(mode="json", exclude={"content_hash"})
            body.update(symbols=["MSFT"], pool=["MSFT"])
            forged = context.__class__(**body, content_hash=digest(body))
            with pytest.raises(ValueError, match="证据"):
                await store.start_stage(stage_id, forged)
            assert (await store.row(STAGE_RUNS, stage_id))["status"] == "pending"
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_empty_pool_pipeline_and_restart_require_no_model_credentials(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path
) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        settings = postgres_settings.model_copy(update={"workspace": "empty-" + new_id(), "artifacts_path": tmp_path})
        try:
            await initialize_schema(engine)
            analyses = analyses_for(snapshot)
            assert all(not report.pool for report in analyses.values())
            first, second = await asyncio.gather(
                run_preview(engine, settings, snapshot, analyses, snapshot.as_of.replace(day=23)),
                run_preview(engine, settings, snapshot, analyses, snapshot.as_of.replace(day=23)),
                return_exceptions=True,
            )
            successes = [result for result in (first, second) if isinstance(result, dict)]
            assert successes and successes[0]["status"] == "completed"
            report = successes[0]
            assert len(report["selections"]) == 2
            assert len(report["reviews"]) == 2
            assert all(stage["attempt_count"] == 1 for stage in report["stages"])
            loaded = await run_preview(engine, settings, snapshot, analyses, snapshot.as_of.replace(day=23))
            assert loaded == report
            forced = await run_preview(
                engine,
                settings,
                snapshot,
                analyses,
                snapshot.as_of.replace(day=23),
                force=True,
                parent_id=report["run_id"],
            )
            assert forced["run_id"] != report["run_id"]
            assert forced["manifest"]["parent_id"] == report["run_id"]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
