"""盘前专用身份、持久意图和真实原生PG/行情/量化执行；无外部调用。"""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine, delete, func, select, update

from stock_research.config import Settings
from stock_research.data import DAILY_BARS
from stock_research.errors import ResearchConflict
from stock_research.execution_budget import run_deadline
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.premarket_service import cancel_job, job_receipt, request_identity
from stock_research.run_manager import RunManager
from stock_research.run_store import RunStore
from stock_research.schema import API_REQUESTS, PREPARATION_ATTEMPTS, QUANT_ATTEMPTS, RESEARCH_RUNS
from stock_research.schemas.premarket import PremarketInput
from stock_research.store import close_engine, database_engine, initialize_schema
from stock_research.workflow_client import PremarketClient
from stock_research.workflow_settings import WorkflowSettings
from tests.stock_research.test_http_api import TOKEN
from tests.stock_research.test_preparations import READER
from tests.stock_research.test_preparations import preparation_settings as preparation_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
WORKFLOW_TOKEN = "synthetic-premarket-identity-over-32-characters"
FIXED = datetime(2026, 11, 23, 13, tzinfo=UTC)


class Clock:
    def __init__(self) -> None:
        self.at = FIXED

    def now(self) -> datetime:
        return self.at


def intent(stamp: str = "2026-11-23T13:00:00Z", kind: str = "scheduled", workspace: str = "ws-beta") -> PremarketInput:
    return PremarketInput.model_validate(
        {
            "schedule": {
                "schedule_id": "stock-research-premarket",
                "workspace_id": workspace,
                "event_id": "stock-research-premarket:" + ("manual-" if kind == "manual" else "") + stamp,
                "scheduled_for": stamp,
                "trigger_kind": kind,
            }
        }
    )


@pytest.fixture
def configured(
    preparation_settings: Settings, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> tuple[Settings, Clock]:
    clock = Clock()
    for module in ("premarket_service", "pipeline", "quant_jobs", "run_manager", "queries", "premarket_workflow"):
        monkeypatch.setattr("stock_research." + module + ".now", clock.now)
    settings = preparation_settings.model_copy(
        update={
            "api_workflow_token": SecretStr(WORKFLOW_TOKEN),
            "premarket_source_workspace": "ws-beta",
            "model_credentials_path": tmp_path / "no-model-identity",
        }
    )
    return settings, clock


async def seed_intent(manager: RunManager, data: PremarketInput, at: datetime = FIXED) -> str:
    """造已接收但冻结前中断的真实请求，时间显式固定，不修改意图载荷。"""
    key, body = request_identity(data)
    async with manager.requests.claim(key, body) as row:
        job_id = row["uuid"]
    async with manager.store.engine.begin() as connection:
        await connection.execute(update(API_REQUESTS).where(API_REQUESTS.c.uuid == job_id).values(create_time=at))
    return job_id


async def settled(client: httpx.AsyncClient, job_id: str) -> dict[str, object]:
    async with asyncio.timeout(20):
        while True:
            response = await client.get("/v1/premarket-jobs/" + job_id)
            assert response.status_code == 200, response.text
            receipt = response.json()["data"]
            if receipt["status"] not in {"pending", "running"}:
                return receipt
            await asyncio.sleep(0.02)


@pytest.mark.parametrize("change", ["short", "operator", "reader", "workspace"])
def test_workflow_identity_is_independent_and_source_bound(change: str) -> None:
    values = {
        "api_token": TOKEN,
        "api_read_token": READER,
        "api_workflow_token": WORKFLOW_TOKEN,
        "premarket_source_workspace": "ws-beta",
    }
    if change == "workspace":
        values["premarket_source_workspace"] = ""
    else:
        values["api_workflow_token"] = {"short": "short", "operator": TOKEN, "reader": READER}[change]
    with pytest.raises(ValueError, match="盘前身份"):
        create_app(Settings(_env_file=None, **values))


def test_scoped_identity_matrix_and_durable_holiday_skip(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, _ = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent("2026-11-26T13:00:00Z")
                first = await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                assert first.status_code == 202
                receipt = first.json()["data"]
                assert receipt["status"] == "skipped" and receipt["run"] is None and receipt["preparation"] is None
                second = await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                assert second.json()["data"] == receipt
                assert (await client.get("/v1/premarket-jobs/" + receipt["job_id"])).json()["data"] == receipt
                for method, path in [
                    ("POST", "/v1/research-runs"),
                    ("GET", "/v1/reports/latest"),
                    ("GET", "/v1/chat-turns"),
                    ("GET", "/v1/premarket-jobs"),
                    ("POST", "/v1/parameter-sets"),
                    ("POST", "/v1/theme-proposals"),
                    ("POST", "/v1/premarket-jobs/" + receipt["job_id"] + "/inputs"),
                ]:
                    assert (await client.request(method, path, json={})).status_code == 403
                assert (await client.get("/v1/premarket-jobs/" + new_id())).status_code == 404
                assert (
                    await client.post("/v1/premarket-jobs", json=intent(workspace="other").model_dump(mode="json"))
                ).status_code == 404
                for identity in (READER, ""):
                    code = 403 if identity else 401
                    assert (
                        await client.get(
                            "/v1/premarket-jobs/" + receipt["job_id"], headers={"Authorization": "Bearer " + identity}
                        )
                    ).status_code == code
                assert (
                    await client.post(
                        "/v1/premarket-jobs",
                        json=data.model_dump(mode="json"),
                        headers={"Origin": "https://untrusted.invalid"},
                    )
                ).status_code == 403
                assert WORKFLOW_TOKEN not in first.text and "receipt_path" not in first.text
                assert (await client.post("/v1/premarket-jobs/" + receipt["job_id"] + "/retry")).status_code == 409
            store = RunManager(RunStore(engine, settings.workspace), settings)
            assert (await job_receipt(store, receipt["job_id"])).status == "skipped"
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(RESEARCH_RUNS)
                        .where(RESEARCH_RUNS.c.workspace == settings.workspace)
                    )
                    == 0
                )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_actual_quant_execution_idempotency_client_and_frozen_budget(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                responses = await asyncio.gather(
                    *(client.post("/v1/premarket-jobs", json=data.model_dump(mode="json")) for _ in range(2))
                )
                assert all(response.status_code == 202 for response in responses)
                assert {response.json()["data"]["job_id"] for response in responses} == {job_id}
                final = await settled(client, job_id)
                assert final["status"] == "completed" and len(final["run"]["selections"]) == 2
                assert all(job["status"] == "completed" for job in final["run"]["quant"])
                assert (
                    final["preparation"]["status"] == "ready"
                    and final["preparation"]["run_id"] == final["run"]["run_id"]
                )
                row = await app.state.store.row(RESEARCH_RUNS, final["run"]["run_id"])
                assert row["payload"]["premarket"]["job_id"] == job_id
                assert run_deadline(row, 3000) == FIXED + timedelta(minutes=50)
                reader = PremarketClient(
                    WorkflowSettings(_env_file=None, token=WORKFLOW_TOKEN), data, transport=httpx.ASGITransport(app=app)
                )
                assert (await reader.call("GET", job_id=UUID(job_id))).report.report_id == UUID(
                    final["report"]["report_id"]
                )
                original = final["report"]
                clock.at += timedelta(hours=1)
                replay = (await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))).json()["data"]
                assert replay["report"] == original and replay["run"]["run_id"] == final["run"]["run_id"]
                assert (await client.post("/v1/premarket-jobs/" + job_id + "/retry")).status_code == 409
                async with engine.connect() as connection:
                    for table in (API_REQUESTS, RESEARCH_RUNS, PREPARATION_ATTEMPTS):
                        assert (
                            await connection.scalar(
                                select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                            )
                            == 1
                        )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_quality_failure_attempts_cancel_and_replay_survive_new_owner(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, _ = configured
        source = create_engine("sqlite:///" + str(settings.local_market_path))
        with source.begin() as connection:
            connection.execute(delete(DAILY_BARS).where(DAILY_BARS.c.kind == "stock"))
        source.dispose()
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                for _ in range(2):
                    response = await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                    assert response.status_code == 202
                    receipt = response.json()["data"]
                    assert receipt["status"] == "failed_preparation" and receipt["run"] is None
                    assert receipt["preparation"]["error_code"] == "quality_failed"
                other = RunManager(app.state.store, settings)
                key, body = request_identity(data)
                async with app.state.manager.requests.claim(key, body):
                    with pytest.raises(ResearchConflict):
                        await cancel_job(other, job_id)
                assert (await client.post("/v1/premarket-jobs/" + job_id + "/cancel")).json()["data"][
                    "status"
                ] == "cancelled"
                assert (await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))).json()["data"][
                    "status"
                ] == "cancelled"
                assert (await job_receipt(other, job_id)).status == "cancelled"
                async with engine.connect() as connection:
                    assert (
                        await connection.scalar(
                            select(func.count())
                            .select_from(PREPARATION_ATTEMPTS)
                            .where(PREPARATION_ATTEMPTS.c.workspace == settings.workspace)
                        )
                        == 2
                    )
                    assert (
                        await connection.scalar(
                            select(func.count())
                            .select_from(RESEARCH_RUNS)
                            .where(RESEARCH_RUNS.c.workspace == settings.workspace)
                        )
                        == 0
                    )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_partial_job_cancel_seals_original_intent_without_changing_saved_results(
    configured: tuple[Settings, Clock],
) -> None:
    async def scenario() -> None:
        settings, _ = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                original = await settled(client, job_id)
                run_id = original["run"]["run_id"]
                original_selections = original["run"]["selections"]
                await app.state.store.publish(run_id, "partial")
                retried = await client.post("/v1/premarket-jobs/" + job_id + "/retry")
                assert retried.status_code == 202
                recovered = await settled(client, job_id)
                assert recovered["status"] == "completed" and recovered["run"]["selections"] == original_selections
                assert recovered["plan"] == original["plan"]
                async with engine.connect() as connection:
                    attempts = await connection.scalar(
                        select(func.count())
                        .select_from(QUANT_ATTEMPTS)
                        .where(QUANT_ATTEMPTS.c.workspace == settings.workspace)
                    )
                assert attempts == 2
                await app.state.store.publish(run_id, "partial")
                cancelled = (await client.post("/v1/premarket-jobs/" + job_id + "/cancel")).json()["data"]
                assert cancelled["status"] == "cancelled" and cancelled["run"]["status"] == "partial"
                assert cancelled["run"]["selections"] == original_selections
                assert (await client.post("/v1/premarket-jobs/" + job_id + "/retry")).status_code == 409
                replay = (await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))).json()["data"]
                assert replay["status"] == "cancelled" and replay["run"]["run_id"] == run_id
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_real_quant_workers_stop_at_original_deadline_and_cannot_receive_fresh_budget(
    configured: tuple[Settings, Clock], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        processes = []
        original_start = RunManager.start

        def near_deadline(manager: RunManager, run_id: str) -> None:
            clock.at = FIXED + timedelta(minutes=50, milliseconds=-30)
            original_start(manager, run_id)

        original_spawn = asyncio.create_subprocess_exec

        async def capture_spawn(*args, **kwargs):
            process = await original_spawn(*args, **kwargs)
            processes.append(process)
            return process

        monkeypatch.setattr(RunManager, "start", near_deadline)
        monkeypatch.setattr("stock_research.quant_execution.asyncio.create_subprocess_exec", capture_spawn)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                result = await settled(client, job_id)
                assert result["status"] == "failed"
                assert all(
                    job["status"] == "failed" and job["error_code"] == "quant_timeout" for job in result["run"]["quant"]
                )
                assert processes and all(process.returncode is not None for process in processes)
                for process in processes:
                    with pytest.raises(ProcessLookupError):
                        os.kill(process.pid, 0)
                    with pytest.raises(ProcessLookupError):
                        os.killpg(process.pid, 0)
                clock.at = FIXED + timedelta(minutes=50)
                assert (await client.post("/v1/premarket-jobs/" + job_id + "/retry")).status_code == 409
                assert (await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))).json()["data"][
                    "run"
                ]["run_id"] == result["run"]["run_id"]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_model_context_uses_original_cutoff_after_quant_is_already_available(
    configured: tuple[Settings, Clock], snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stock_research.pipeline import prepare_preview
    from stock_research.preparations import record_preparation
    from tests.stock_research.test_run_store import analyses_for

    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        original_start = RunManager.start
        from stock_research import pipeline

        original_context = pipeline.build_context
        contexts = []

        def capture_context(*args, **kwargs):
            context = original_context(*args, **kwargs)
            contexts.append(context)
            return context

        monkeypatch.setattr(pipeline, "build_context", capture_context)

        async def prepared(store, config, request, request_id, *, premarket):
            async with record_preparation(store, request.target_session, "api", request_id) as attempt:
                await attempt.checkpoint("snapshot", snapshot.quality)
                run_id, _ = await prepare_preview(
                    store.engine,
                    config,
                    snapshot,
                    analyses_for(snapshot),
                    request.target_session,
                    request_id=request_id,
                    premarket=premarket,
                )
                await store.publish(run_id, "pending")
                await attempt.ready(run_id)
                return run_id

        def near_deadline(manager: RunManager, run_id: str) -> None:
            clock.at = FIXED + timedelta(minutes=49, seconds=59)
            original_start(manager, run_id)

        monkeypatch.setattr("stock_research.premarket_service.prepare_from_market", prepared)
        monkeypatch.setattr(RunManager, "start", near_deadline)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                assert (await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))).status_code == 202
                result = await settled(client, job_id)
                assert result["status"] == "completed"
                summary = await app.state.store.summary(result["run"]["run_id"])
                assert len(summary["stages"]) == 4
                assert len(contexts) == 4
                assert all(context.deadline == FIXED + timedelta(minutes=50) for context in contexts)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_new_owner_recovers_frozen_run_when_request_binding_was_not_committed(
    configured: tuple[Settings, Clock],
) -> None:
    from stock_research.identity import digest
    from stock_research.premarket_service import admit_job

    async def scenario() -> None:
        settings, _ = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                original = await settled(client, job_id)
                request = await app.state.store.row(API_REQUESTS, job_id)
                pending_payload = {**request["payload"], "run_id": None}
                async with engine.begin() as connection:
                    await connection.execute(
                        update(API_REQUESTS)
                        .where(API_REQUESTS.c.uuid == job_id)
                        .values(run_uuid=None, payload=pending_payload, content_hash=digest(pending_payload))
                    )
                other = RunManager(RunStore(engine, settings.workspace), settings)
                recovered = await admit_job(other, data)
                assert str(recovered.run.run_id) == original["run"]["run_id"] and recovered.status == "completed"
                assert str(recovered.report.report_id) == original["report"]["report_id"]
                rebound = await other.store.row(API_REQUESTS, job_id)
                assert rebound["run_uuid"] == original["run"]["run_id"]
                async with engine.connect() as connection:
                    assert (
                        await connection.scalar(
                            select(func.count())
                            .select_from(RESEARCH_RUNS)
                            .where(RESEARCH_RUNS.c.workspace == settings.workspace)
                        )
                        == 1
                    )
                    assert (
                        await connection.scalar(
                            select(func.count())
                            .select_from(QUANT_ATTEMPTS)
                            .where(QUANT_ATTEMPTS.c.workspace == settings.workspace)
                        )
                        == 2
                    )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
