"""原生PG和实际量化worker：局部失败、冻结历史和只补失败任务。"""

import asyncio
from pathlib import Path

import pytest
from sqlalchemy import func, select

from stock_research import quant_jobs
from stock_research.config import Settings
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.observations import stock_observation
from stock_research.pipeline import prepare_preview, resume_preview
from stock_research.quant_execution import QuantExecutionError
from stock_research.queries import latest_report_id, report_view
from stock_research.report_queries import group_strength, market_overview, strategy_rules
from stock_research.run_store import RunStore
from stock_research.schema import QUANT_ATTEMPTS, QUANT_JOBS, RESEARCH_REPORTS, RESEARCH_RUNS
from stock_research.schemas.universe import UniverseQuery
from stock_research.store import close_engine, database_engine, initialize_schema
from stock_research.universe_queries import universe_page

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def test_quant_failure_continues_and_retry_preserves_success_and_old_report(
    postgres_settings: Settings,
    snapshot: Snapshot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={"workspace": "quant-recovery-" + new_id(), "artifacts_path": tmp_path}
        )
        engine = database_engine(settings, test=True)
        original = quant_jobs.analyze_isolated
        failed = True
        seen = []

        async def controlled(config, path, frozen, strategy, **kwargs):
            seen.append(strategy)
            if strategy == "martin" and failed:
                raise QuantExecutionError("quant_calculation_failed")
            return await original(config, path, frozen, strategy, **kwargs)

        monkeypatch.setattr(quant_jobs, "analyze_isolated", controlled)
        try:
            await initialize_schema(engine)
            store = RunStore(engine, settings.workspace)
            run_id, _ = await prepare_preview(engine, settings, snapshot, None, snapshot.as_of.replace(day=23))
            before = await store.row(RESEARCH_RUNS, run_id)
            assert not before["payload"]["factor_results"]
            initial = await report_view(store, await latest_report_id(store, run_id=run_id))
            assert initial.run.status == "pending" and all(row.status == "pending" for row in initial.run.quant)
            partial = await resume_preview(engine, settings, run_id)
            assert partial["status"] == "partial" and partial["error_code"] == "quant_unavailable"
            assert [row["strategy"] for row in partial["selections"]] == ["wyckoff"]
            assert partial["selections"][0]["payload"]["source"] == "empty_pool"
            async with engine.connect() as connection:
                old_id = await connection.scalar(
                    select(RESEARCH_REPORTS.c.uuid)
                    .where(RESEARCH_REPORTS.c.run_uuid == run_id)
                    .order_by(RESEARCH_REPORTS.c.revision.desc())
                    .limit(1)
                )
            historical = await report_view(store, old_id)
            unavailable = await stock_observation(store, run_id, "AAPL", 2, old_id)
            assert unavailable.observations[0].artifact_id is None
            assert unavailable.observations[0].in_pool is None
            assert unavailable.observations[0].quant.status == "failed"
            assert len(unavailable.observations) == 2
            assert (await group_strength(store, old_id, 1, 20)).quant.status == "failed"
            assert (await market_overview(store, old_id, 2)).strategies[0].calculated_count is None
            assert (await strategy_rules(store, old_id)).strategies[0].artifact_id is None
            assert (await universe_page(store, old_id, UniverseQuery())).items[0].strategies[0].in_pool is None
            failed = False
            # 磁盘上当前参数不可用，补算仍必须只使用原run的冻结参数。
            missing = settings.model_copy(
                update={
                    "parameters_path": tmp_path / "absent.json",
                    "wyckoff_parameters_path": tmp_path / "absent2.json",
                }
            )
            recovered = await resume_preview(engine, missing, run_id)
            assert recovered["status"] == "completed" and recovered["error_code"] is None
            assert partial["selections"][0] in recovered["selections"]
            assert (
                recovered["manifest"]["factor_results"]["wyckoff"] == partial["manifest"]["factor_results"]["wyckoff"]
            )
            assert seen == ["martin", "wyckoff", "martin"]
            assert (await store.row(RESEARCH_RUNS, run_id))["content_hash"] == before["content_hash"]
            assert (await report_view(store, old_id)).model_dump() == historical.model_dump()
            assert (await stock_observation(store, run_id, "AAPL", 2, old_id)).model_dump() == unavailable.model_dump()
            async with engine.connect() as connection:
                counts = (
                    await connection.execute(
                        select(QUANT_JOBS.c.strategy, QUANT_JOBS.c.attempt_count).where(QUANT_JOBS.c.run_uuid == run_id)
                    )
                ).all()
                assert dict(counts) == {"martin": 2, "wyckoff": 1}
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(QUANT_ATTEMPTS)
                        .where(QUANT_ATTEMPTS.c.workspace == settings.workspace)
                    )
                    == 3
                )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_nonempty_selection_continues_when_other_quant_fails(
    postgres_settings: Settings,
    snapshot: Snapshot,
    trending_bars,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from stock_research import pipeline
    from stock_research.decisions import EvidenceClaim, ResearchPoint, SelectionDecision, SelectionItem
    from stock_research.executor import StageResult
    from stock_research.watch_queries import watch_page
    from tests.stock_research.test_watchlists import trending_snapshot

    async def scenario() -> None:
        valid = trending_snapshot(snapshot, trending_bars)
        trusted = tmp_path / "model.env"
        trusted.write_text(
            "DSH_E2E_MODEL_PROVIDER=fixture\nDSH_E2E_MODEL_ID=fixture\nDSH_E2E_MODEL_BASE_URL=https://example.invalid/v1\nDSH_E2E_MODEL_API_KEY=fixture\n"
        )
        settings = postgres_settings.model_copy(
            update={"workspace": "quant-nonempty-" + new_id(), "artifacts_path": tmp_path, "model_env_path": trusted}
        )
        engine = database_engine(settings, test=True)
        original, called = quant_jobs.analyze_isolated, []

        async def controlled_quant(config, path, frozen, strategy, **kwargs):
            if strategy == "wyckoff":
                raise QuantExecutionError("quant_calculation_failed")
            return await original(config, path, frozen, strategy, **kwargs)

        async def verified(_path):
            return None

        async def controlled_model(_path, context, _connection, **_kwargs):
            called.append((context.strategy, context.role))
            assert context.strategy == "martin" and context.role == "select"
            ref = "martin/AAPL/factors/close"
            point = ResearchPoint(text="合成模型验证正式初选提交", evidence_refs=(ref,))
            item = SelectionItem(
                symbol="AAPL",
                rationale=point,
                risks=(point,),
                confirmation_conditions=(point,),
                claims=(EvidenceClaim(ref=ref, value=context.resolve(ref).value),),
            )
            decision = SelectionDecision(strategy="martin", context_hash=context.content_hash, items=(item,))
            return StageResult(
                context_hash=context.content_hash,
                status="completed",
                decision=decision,
                error=None,
                receipt_path="controlled-model",
            )

        monkeypatch.setattr(quant_jobs, "analyze_isolated", controlled_quant)
        monkeypatch.setattr(pipeline, "verify_runtime", verified)
        monkeypatch.setattr(pipeline, "execute_stage", controlled_model)
        try:
            await initialize_schema(engine)
            run_id, _ = await prepare_preview(engine, settings, valid, None, valid.as_of.replace(day=23))
            result = await resume_preview(engine, settings, run_id)
            assert result["status"] == "partial"
            assert called == [("martin", "select")]
            assert result["selections"][0]["payload"]["decision"]["items"][0]["symbol"] == "AAPL"
            assert not result["reviews"]
            assert [row["strategy"] for row in result["selections"]] == ["martin"]
            watch = await watch_page(RunStore(engine, settings.workspace), 1, 20)
            assert watch.items[0].symbol == "AAPL" and watch.items[0].state == "new"
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_quant_claim_cas_interrupted_attempt_and_both_failed_recovery(
    postgres_settings: Settings,
    snapshot: Snapshot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from stock_research.errors import ResearchConflict
    from stock_research.quant_queries import quant_attempt_page
    from stock_research.quant_store import finish_job, job_rows, start_job
    from stock_research.schema import FACTOR_RESULTS

    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={"workspace": "quant-cas-" + new_id(), "artifacts_path": tmp_path}
        )
        engine = database_engine(settings, test=True)
        original = quant_jobs.analyze_isolated

        async def failed(*_args, **_kwargs):
            raise QuantExecutionError("quant_calculation_failed")

        try:
            await initialize_schema(engine)
            store = RunStore(engine, settings.workspace)
            run_id, _ = await prepare_preview(engine, settings, snapshot, None, snapshot.as_of.replace(day=23))
            run = await store.row(RESEARCH_RUNS, run_id)
            rows = await job_rows(store, run)
            winners = await asyncio.gather(start_job(store, rows[0]), start_job(store, rows[0]), return_exceptions=True)
            assert sum(isinstance(row, ResearchConflict) for row in winners) == 1
            winner = next(row for row in winners if isinstance(row, dict))
            await finish_job(store, run, winner, error="quant_calculation_failed")
            with pytest.raises(ResearchConflict):
                await finish_job(store, run, winner, error="quant_calculation_failed")
            await start_job(store, rows[1])
            monkeypatch.setattr(quant_jobs, "analyze_isolated", failed)
            failed_run = await resume_preview(engine, settings, run_id)
            assert failed_run["status"] == "failed" and not failed_run["selections"]
            history = await quant_attempt_page(store, run_id, 1, 20)
            assert history.total == 4 and history.recorded
            assert sum(item.error_code == "previous_executor_stopped" for item in history.items) == 1
            assert all(
                row.status == "failed"
                for row in (await report_view(store, (await latest_report_id(store, run_id=run_id)))).run.quant
            )
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(FACTOR_RESULTS)
                        .where(FACTOR_RESULTS.c.workspace == settings.workspace)
                    )
                    == 0
                )
            monkeypatch.setattr(quant_jobs, "analyze_isolated", original)
            assert (await resume_preview(engine, settings, run_id))["status"] == "completed"
            assert (await store.row(RESEARCH_RUNS, run_id))["content_hash"] == run["content_hash"]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_quant_history_http_isolation_and_cancelled_job_recovery(
    postgres_settings: Settings,
    snapshot: Snapshot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    from pydantic import SecretStr
    from sqlalchemy import update

    from stock_research.http_api import create_app
    from stock_research.query_client import QueryError, ResearchReader
    from stock_research.query_settings import QuerySettings
    from tests.stock_research.test_queries import OPERATOR, READER

    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={
                "workspace": "quant-cancel-" + new_id(),
                "artifacts_path": tmp_path,
                "postgres_database": "stock_research_test",
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
            }
        )
        engine = database_engine(settings)
        original, entered = quant_jobs.analyze_isolated, asyncio.Event()

        async def interrupted(*_args, **_kwargs):
            entered.set()
            await asyncio.Event().wait()

        try:
            await initialize_schema(engine)
            run_id, _ = await prepare_preview(engine, settings, snapshot, None, snapshot.as_of.replace(day=23))
            monkeypatch.setattr(quant_jobs, "analyze_isolated", interrupted)
            task = asyncio.create_task(resume_preview(engine, settings, run_id))
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            store = RunStore(engine, settings.workspace)
            cancelled = await store.summary(run_id)
            assert cancelled["status"] == "cancelled"
            assert cancelled["manifest"]["quant"][0]["status"] == "cancelled"
            old_id = await latest_report_id(store, run_id=run_id)
            old = await report_view(store, old_id)
            monkeypatch.setattr(quant_jobs, "analyze_isolated", original)
            assert (await resume_preview(engine, settings, run_id))["status"] == "completed"
            app = create_app(settings)
            transport = httpx.ASGITransport(app=app)
            reader = ResearchReader(QuerySettings(_env_file=None, token=READER), transport=transport)
            async with app.router.lifespan_context(app):
                page1, page2 = (
                    await reader.quant_attempts(old.run.run_id, 1, 2),
                    await reader.quant_attempts(old.run.run_id, 2, 2),
                )
                assert page1.recorded and page1.total == 3 and len(page1.items) == 2 and len(page2.items) == 1
                assert len({item.id for item in (*page1.items, *page2.items)}) == 3
                assert any(item.status == "cancelled" for item in (*page1.items, *page2.items))
                assert (await reader.report(old.report_id)).model_dump() == old.model_dump()
                async with engine.begin() as connection:
                    await connection.execute(
                        update(QUANT_ATTEMPTS)
                        .where(QUANT_ATTEMPTS.c.uuid == str(page1.items[0].id))
                        .values(payload={"invalid": "synthetic-private-value"})
                    )
                with pytest.raises(QueryError):
                    await reader.quant_attempts(old.run.run_id)
                assert (await reader.report(old.report_id)).content_hash == old.content_hash
            foreign = create_app(settings.model_copy(update={"workspace": "foreign"}))
            async with foreign.router.lifespan_context(foreign):
                outsider = ResearchReader(
                    QuerySettings(_env_file=None, token=READER), transport=httpx.ASGITransport(app=foreign)
                )
                with pytest.raises(QueryError):
                    await outsider.quant_attempts(old.run.run_id)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_same_content_snapshot_ids_do_not_cross_workspace_bindings(
    postgres_settings: Settings,
    snapshot: Snapshot,
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        try:
            await initialize_schema(engine)
            outputs = []
            for _ in range(2):
                settings = postgres_settings.model_copy(
                    update={"workspace": "quant-scope-" + new_id(), "artifacts_path": tmp_path}
                )
                run_id, _ = await prepare_preview(engine, settings, snapshot, None, snapshot.as_of.replace(day=23))
                result = await resume_preview(engine, settings, run_id)
                assert result["status"] == "completed"
                observed = await stock_observation(RunStore(engine, settings.workspace), run_id, "AAPL", 2)
                assert observed.snapshot_hash == snapshot.content_hash
                outputs.append(observed.snapshot_id)
            assert outputs[0] != outputs[1]
            assert len(list((tmp_path / "analyses").glob("*.json"))) == 4
            from stock_research.pipeline import run_preview
            from tests.stock_research.test_run_store import analyses_for

            analyses = analyses_for(snapshot)
            original_hashes = {key: value.content_hash for key, value in analyses.items()}
            legacy = postgres_settings.model_copy(
                update={"workspace": "quant-legacy-" + new_id(), "artifacts_path": tmp_path}
            )
            result = await run_preview(engine, legacy, snapshot, analyses, snapshot.as_of.replace(day=23))
            observed = await stock_observation(RunStore(engine, legacy.workspace), result["run_id"], "AAPL", 2)
            assert observed.snapshot_id not in outputs and str(observed.snapshot_id) != snapshot.id
            assert all(
                value.snapshot_id == snapshot.id and value.content_hash == original_hashes[key]
                for key, value in analyses.items()
            )
            legacy_report = await report_view(
                RunStore(engine, legacy.workspace),
                await latest_report_id(RunStore(engine, legacy.workspace), run_id=result["run_id"]),
            )
            assert all(row.id is None and row.attempt_count is None for row in legacy_report.run.quant)

        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_failed_quant_preserves_prior_day_watch_state_with_unreviewed_marker(
    postgres_settings: Settings,
    snapshot: Snapshot,
    trending_bars,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import date, timedelta

    from stock_research import pipeline
    from stock_research.analysis import compute_martin
    from stock_research.decisions import (
        EvidenceClaim,
        ResearchPoint,
        ReviewDecision,
        ReviewItem,
        SelectionDecision,
        SelectionItem,
    )
    from stock_research.executor import StageResult
    from stock_research.parameters import MartinParameters
    from stock_research.pipeline import run_preview
    from stock_research.quality import build_snapshot
    from stock_research.watch_queries import watch_page
    from stock_research.wyckoff import WyckoffParameters, compute_wyckoff
    from tests.stock_research.test_watchlists import trending_snapshot

    async def scenario() -> None:
        valid = trending_snapshot(snapshot, trending_bars)
        trusted = tmp_path / "model.env"
        trusted.write_text(
            "DSH_E2E_MODEL_PROVIDER=fixture\nDSH_E2E_MODEL_ID=fixture\nDSH_E2E_MODEL_BASE_URL=https://example.invalid/v1\nDSH_E2E_MODEL_API_KEY=fixture\n"
        )
        settings = postgres_settings.model_copy(
            update={"workspace": "quant-watch-" + new_id(), "artifacts_path": tmp_path, "model_env_path": trusted}
        )
        engine = database_engine(settings, test=True)
        original = quant_jobs.analyze_isolated

        async def verified(_path):
            return None

        async def controlled_model(_path, context, _connection, **kwargs):
            if context.role == "select":
                ref = "martin/AAPL/factors/close"
                point = ResearchPoint(text="合成历史初选", evidence_refs=(ref,))
                item = SelectionItem(
                    symbol="AAPL",
                    rationale=point,
                    risks=(point,),
                    confirmation_conditions=(point,),
                    claims=(EvidenceClaim(ref=ref, value=context.resolve(ref).value),),
                )
                decision = SelectionDecision(strategy="martin", context_hash=context.content_hash, items=(item,))
            else:
                ref = "wyckoff/AAPL/calculation_available"
                point = ResearchPoint(text="本策略结构资料不足", evidence_refs=(ref,))
                item = ReviewItem(
                    symbol="AAPL",
                    stance="insufficient_evidence",
                    rationale=point,
                    objections=(),
                    confirmation_conditions=(),
                    claims=(EvidenceClaim(ref=ref, value=False),),
                    data_limitations=context.limitations,
                )
                decision = ReviewDecision(
                    strategy="wyckoff",
                    context_hash=context.content_hash,
                    target_selection_id=kwargs["target"]["selection_id"],
                    items=(item,),
                )
            return StageResult(
                context_hash=context.content_hash,
                status="completed",
                decision=decision,
                error=None,
                receipt_path="controlled-model",
            )

        async def failed_martin(config, path, frozen, strategy, **kwargs):
            if strategy == "martin":
                raise QuantExecutionError("quant_calculation_failed")
            return await original(config, path, frozen, strategy, **kwargs)

        monkeypatch.setattr(pipeline, "verify_runtime", verified)
        monkeypatch.setattr(pipeline, "execute_stage", controlled_model)
        try:
            await initialize_schema(engine)
            analyses = {
                "martin": compute_martin(valid, MartinParameters()),
                "wyckoff": compute_wyckoff(valid, WyckoffParameters()),
            }
            prior = await run_preview(engine, settings, valid, analyses, date(2026, 11, 23))
            assert prior["status"] == "completed"
            store = RunStore(engine, settings.workspace)
            previous = (await watch_page(store, 1, 20)).items[0]
            latest = {}
            for bar in valid.bars:
                latest[bar.kind, bar.symbol] = bar
            added = tuple(
                bar.model_copy(
                    update={
                        "session_date": date(2026, 11, 23),
                        "observed_time": bar.observed_time + timedelta(days=3),
                        "source_revision": bar.source_revision + timedelta(days=3),
                    }
                )
                for bar in latest.values()
            )
            current = build_snapshot(
                valid.universe, valid.classifications, valid.bars + added, "synthetic-next-day", date(2026, 11, 23)
            )
            monkeypatch.setattr(quant_jobs, "analyze_isolated", failed_martin)
            run_id, _ = await prepare_preview(engine, settings, current, None, date(2026, 11, 24))
            assert (await resume_preview(engine, settings, run_id))["status"] == "partial"
            kept = (await watch_page(store, 1, 20)).items[0]
            assert kept.state == previous.state and kept.state_run_id == previous.state_run_id
            assert kept.evidence_refs == previous.evidence_refs and kept.state_as_of == previous.state_as_of
            assert kept.checked_run_id != previous.checked_run_id and kept.not_reviewed
            assert kept.reason == "quant_unavailable" and kept.target_session == date(2026, 11, 24)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_real_worker_signal_recovers_in_original_run(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【异常值】默认生产入口实际终止一次worker，落库错误并自动补齐同一研究。"""
    import os
    import sys

    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={"workspace": "quant-signal-" + new_id(), "artifacts_path": tmp_path}
        )
        engine = database_engine(settings, test=True)
        original, children = asyncio.create_subprocess_exec, []
        code = (
            "import os,signal; import stock_research.quant_worker as worker; "
            "worker.worker_compute=lambda request: os.kill(os.getpid(),signal.SIGTERM); "
            "raise SystemExit(worker.main())"
        )

        async def interrupt_once(*args, **kwargs):
            child = (
                await original(sys.executable, "-c", code, **kwargs)
                if not children
                else await original(*args, **kwargs)
            )
            children.append(child)
            return child

        monkeypatch.setattr(asyncio, "create_subprocess_exec", interrupt_once)
        try:
            await initialize_schema(engine)
            store = RunStore(engine, settings.workspace)
            run_id, _ = await prepare_preview(engine, settings, snapshot, None, snapshot.as_of.replace(day=23))
            before = await store.row(RESEARCH_RUNS, run_id)
            initial = await report_view(store, await latest_report_id(store, run_id=run_id))
            result = await resume_preview(engine, settings, run_id)
            assert result["status"] == "completed" and len(result["stages"]) == 4
            assert len(children) == 3 and children[0].returncode == -15
            assert all(child.returncode is not None for child in children)
            for child in children:
                with pytest.raises(ProcessLookupError):
                    os.kill(child.pid, 0)
            async with engine.connect() as connection:
                jobs = (
                    (await connection.execute(select(QUANT_JOBS).where(QUANT_JOBS.c.run_uuid == run_id)))
                    .mappings()
                    .all()
                )
                attempts = (
                    (
                        await connection.execute(
                            select(QUANT_ATTEMPTS).where(QUANT_ATTEMPTS.c.job_uuid.in_([row["uuid"] for row in jobs]))
                        )
                    )
                    .mappings()
                    .all()
                )
            assert {row["strategy"]: row["attempt_count"] for row in jobs} == {"martin": 2, "wyckoff": 1}
            failed = next(row for row in attempts if row["status"] == "failed")
            assert failed["payload"]["error_code"] == "quant_process_failed"
            after = await store.row(RESEARCH_RUNS, run_id)
            assert after["content_hash"] == before["content_hash"]
            assert (await report_view(store, str(initial.report_id))).model_dump() == initial.model_dump()
            factors = result["manifest"]["factor_results"]
            assert (await resume_preview(engine, settings, run_id))["manifest"]["factor_results"] == factors
            assert len(children) == 3
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
