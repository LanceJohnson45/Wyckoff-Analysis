"""不同原事件共享首次研究；强制重分析仅由operator显式关联父版本。"""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import func, select, update

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import digest
from stock_research.parameter_store import activate_parameters, register_parameters
from stock_research.parameters import MartinParameters
from stock_research.premarket_service import admit_job, job_receipt
from stock_research.run_manager import RunManager
from stock_research.run_store import RunStore
from stock_research.schema import API_REQUESTS, MARKET_SNAPSHOTS, PREPARATION_ATTEMPTS, QUANT_ATTEMPTS, RESEARCH_RUNS
from stock_research.schemas.parameters import CreateParameterSet
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_http_api import TOKEN
from tests.stock_research.test_premarket_api import WORKFLOW_TOKEN, Clock, intent, seed_intent, settled
from tests.stock_research.test_premarket_api import configured as configured
from tests.stock_research.test_preparations import preparation_settings as preparation_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://127.0.0.1:8788",
        headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
    )


def test_different_manual_events_concurrently_share_first_frozen_run(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app, peer = create_app(settings), create_app(settings)
            async with (
                app.router.lifespan_context(app),
                peer.router.lifespan_context(peer),
                client(app) as http,
                client(peer) as other_http,
            ):
                events = [intent(kind="manual"), intent("2026-11-23T13:01:00Z", kind="manual")]
                ids = [await seed_intent(app.state.manager, item, item.schedule.scheduled_for) for item in events]
                clock.at += timedelta(minutes=1)
                replies = await asyncio.gather(
                    *(
                        connection.post("/v1/premarket-jobs", json=item.model_dump(mode="json"))
                        for connection, item in zip((http, other_http), events, strict=True)
                    )
                )
                assert all(reply.status_code == 202 for reply in replies)
                receipts = [await settled(http, job_id) for job_id in ids]
                for index, receipt in enumerate(receipts):
                    if receipt["status"] == "failed_preparation":
                        assert receipt["preparation"]["error_code"] == "input_unavailable" and receipt["run"] is None
                        replay = await http.post("/v1/premarket-jobs", json=events[index].model_dump(mode="json"))
                        assert replay.status_code == 202
                        receipts[index] = await settled(http, ids[index])
                assert {item["status"] for item in receipts} == {"completed"}
                assert len({item["run"]["run_id"] for item in receipts}) == 1
                assert receipts[0]["report"] == receipts[1]["report"]
                origin = receipts[0]["origin"]
                assert origin == receipts[1]["origin"] and origin["job_id"] in ids
                row = await app.state.store.row(RESEARCH_RUNS, receipts[0]["run"]["run_id"])
                assert row["payload"]["premarket"] == origin
                other = RunManager(RunStore(engine, settings.workspace), settings)
                assert (await job_receipt(other, ids[1])).origin.model_dump(mode="json") == origin
                async with engine.connect() as connection:
                    counts = [
                        await connection.scalar(
                            select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                        )
                        for table in (API_REQUESTS, RESEARCH_RUNS, QUANT_ATTEMPTS)
                    ]
                assert counts == [2, 1, 2]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_completed_manual_alias_reuses_frozen_input_when_current_market_unavailable(
    configured: tuple[Settings, Clock],
) -> None:
    """同日同配置新事件复用原研究，不应再次依赖已经物化的行情源。"""

    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        source = settings.local_market_path
        backup = source.with_suffix(".unavailable")
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                original_id = await seed_intent(app.state.manager, first)
                assert (await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))).status_code == 202
                original = await settled(http, original_id)
                assert original["status"] == "completed"
                run_id = original["run"]["run_id"]
                frozen = await app.state.store.row(RESEARCH_RUNS, run_id)
            source.rename(backup)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                existing = (await http.get("/v1/premarket-jobs/" + original_id)).json()["data"]
                assert existing["report"] == original["report"]
                clock.at += timedelta(minutes=1)
                alias = intent("2026-11-23T13:01:00Z", kind="manual")
                alias_id = await seed_intent(app.state.manager, alias, clock.at)
                response = await http.post("/v1/premarket-jobs", json=alias.model_dump(mode="json"))
                assert response.status_code == 202
                later = await settled(http, alias_id)
                assert later["status"] == "completed", later
                assert later["run"]["run_id"] == run_id
                assert later["report"] == original["report"] and later["origin"] == original["origin"]
                assert later["preparation"]["quality"] == original["preparation"]["quality"]
                assert later["preparation"]["market_refresh"] is None
                assert (await app.state.store.row(RESEARCH_RUNS, run_id)) == frozen
                async with engine.connect() as connection:
                    counts = [
                        await connection.scalar(
                            select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                        )
                        for table in (RESEARCH_RUNS, QUANT_ATTEMPTS, MARKET_SNAPSHOTS)
                    ]
                assert counts == [1, 2, 1]
        finally:
            if backup.exists():
                backup.rename(source)
            await close_engine(engine)

    asyncio.run(scenario())


def test_alias_cancel_seals_origin_and_later_events_without_changing_report(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                first_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, first_id)
                run_id = original["run"]["run_id"]
                await app.state.store.publish(run_id, "partial")
                before = (await http.get("/v1/premarket-jobs/" + first_id)).json()["data"]
                clock.at += timedelta(minutes=1)
                alias = intent("2026-11-23T13:01:00Z", kind="manual")
                alias_id = await seed_intent(app.state.manager, alias, clock.at)
                receipt = (await http.post("/v1/premarket-jobs", json=alias.model_dump(mode="json"))).json()["data"]
                assert receipt["run"]["run_id"] == run_id and receipt["report"] == before["report"]
                cancelled = (await http.post("/v1/premarket-jobs/" + alias_id + "/cancel")).json()["data"]
                assert cancelled["status"] == "cancelled" and cancelled["report"] == before["report"]
                for job_id in (first_id, alias_id):
                    assert (await http.get("/v1/premarket-jobs/" + job_id)).json()["data"]["status"] == "cancelled"
                    assert (await http.post("/v1/premarket-jobs/" + job_id + "/retry")).status_code == 409
                third = intent("2026-11-23T13:02:00Z", kind="manual")
                clock.at += timedelta(minutes=1)
                third_id = await seed_intent(app.state.manager, third, clock.at)
                later = (await http.post("/v1/premarket-jobs", json=third.model_dump(mode="json"))).json()["data"]
                assert later["job_id"] == third_id and later["status"] == "cancelled"
                assert later["origin"]["job_id"] == first_id and later["run"]["run_id"] == run_id
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_new_event_cannot_refresh_original_execution_budget(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                first_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, first_id)
                await app.state.store.publish(original["run"]["run_id"], "partial")
                clock.at += timedelta(hours=1)
                alias = intent("2026-11-23T14:00:00Z", kind="manual")
                alias_id = await seed_intent(app.state.manager, alias, clock.at)
                receipt = (await http.post("/v1/premarket-jobs", json=alias.model_dump(mode="json"))).json()["data"]
                assert receipt["run"]["run_id"] == original["run"]["run_id"]
                assert receipt["plan"]["budget_seconds"] == 3000
                assert receipt["origin"] == original["origin"]
                assert (await http.post("/v1/premarket-jobs/" + alias_id + "/retry")).status_code == 409
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("changed", ["trigger", "configuration", "parameters", "inputs"])
def test_trigger_kind_and_configuration_remain_separate(configured: tuple[Settings, Clock], changed: str) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                first_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, first_id)
                if changed == "configuration":
                    app.state.manager.settings = settings.model_copy(
                        update={"stage_timeout_seconds": settings.stage_timeout_seconds + 1}
                    )
                if changed == "parameters":
                    parameters = MartinParameters.model_validate_json(settings.parameters_path.read_text())
                    candidate = await register_parameters(
                        app.state.store,
                        CreateParameterSet(
                            strategy="martin",
                            parameters=parameters.model_copy(update={"version": "fixture-revised", "support_atr": 0.5}),
                        ),
                    )
                    await activate_parameters(app.state.store, "martin", str(candidate.id), 0)
                if changed == "inputs":
                    baseline = json.loads(settings.classification_path.read_text())
                    baseline["version"] += "-revised"
                    settings.classification_path.write_text(json.dumps(baseline))
                event = intent() if changed == "trigger" else intent("2026-11-23T13:01:00Z", kind="manual")
                clock.at = event.schedule.scheduled_for
                second_id = await seed_intent(app.state.manager, event, clock.at)
                await http.post("/v1/premarket-jobs", json=event.model_dump(mode="json"))
                second = await settled(http, second_id)
                assert second["status"] == "completed" and second["run"]["run_id"] != original["run"]["run_id"]
                assert second["origin"]["job_id"] == second_id
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("quality", [None, {}, {"passed": False}, {"expected_session": "2026-11-19"}])
def test_manual_alias_rejects_invalid_frozen_quality(configured: tuple[Settings, Clock], quality: object) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                first_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, first_id)
                assert original["status"] == "completed"
                run_id = original["run"]["run_id"]
                frozen = await app.state.store.row(RESEARCH_RUNS, run_id)
                payload = {**frozen["payload"], "quality": quality}
                if quality:
                    payload["quality"] = {**frozen["payload"]["quality"], **quality}
                async with engine.begin() as connection:
                    await connection.execute(
                        update(RESEARCH_RUNS)
                        .where(RESEARCH_RUNS.c.uuid == run_id)
                        .values(payload=payload, content_hash=digest(payload))
                    )
                clock.at += timedelta(minutes=1)
                alias = intent("2026-11-23T13:01:00Z", kind="manual")
                alias_id = await seed_intent(app.state.manager, alias, clock.at)
                reply = await http.post("/v1/premarket-jobs", json=alias.model_dump(mode="json"))
                assert reply.status_code == 202
                rejected = await settled(http, alias_id)
                assert rejected["status"] == "failed_preparation" and rejected["run"] is None
                assert rejected["preparation"]["status"] == "failed"
                assert rejected["preparation"]["quality"] is None
                assert (await http.get("/v1/premarket-jobs/" + first_id)).json()["data"]["report"] == original["report"]
                async with engine.connect() as connection:
                    counts = [
                        await connection.scalar(
                            select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                        )
                        for table in (RESEARCH_RUNS, QUANT_ATTEMPTS)
                    ]
                assert counts == [1, 2]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_operator_force_has_parent_and_replays_without_replacing_original(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, _ = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                job_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, job_id)
                parent = original["run"]["run_id"]
                body = {"target_session": "2026-11-23", "force": True, "parent_run_id": parent}
                assert (await http.post("/v1/research-runs", json=body)).status_code == 403
                headers = {"Authorization": "Bearer " + TOKEN, "Idempotency-Key": "explicit-force-from-premarket"}
                first_force = await http.post("/v1/research-runs", json=body, headers=headers)
                assert first_force.status_code == 202, first_force.text
                run_id = first_force.json()["data"]["run_id"]
                assert run_id != parent
                async with asyncio.timeout(20):
                    while (await app.state.store.row(RESEARCH_RUNS, run_id))["status"] in {"pending", "running"}:
                        await asyncio.sleep(0.02)
                replay = await http.post("/v1/research-runs", json=body, headers=headers)
                assert replay.json()["data"]["run_id"] == run_id
                row = await app.state.store.row(RESEARCH_RUNS, run_id)
                assert row["parent_run_uuid"] == parent and row["payload"]["parent_id"] == parent
                assert row["payload"]["manual_nonce"] and "premarket" not in row["payload"]
                assert (await http.get("/v1/premarket-jobs/" + job_id)).json()["data"]["report"] == original["report"]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_alias_ready_before_bind_survives_restart_and_configuration_change(configured: tuple[Settings, Clock]) -> None:
    async def scenario() -> None:
        settings, clock = configured
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with app.router.lifespan_context(app), client(app) as http:
                first = intent(kind="manual")
                job_id = await seed_intent(app.state.manager, first)
                await http.post("/v1/premarket-jobs", json=first.model_dump(mode="json"))
                original = await settled(http, job_id)
                clock.at += timedelta(minutes=1)
                alias = intent("2026-11-23T13:01:00Z", kind="manual")
                alias_id = await seed_intent(app.state.manager, alias, clock.at)
                await http.post("/v1/premarket-jobs", json=alias.model_dump(mode="json"))
                request = await app.state.store.row(API_REQUESTS, alias_id)
                payload = {**request["payload"], "run_id": None}
                async with engine.begin() as connection:
                    await connection.execute(
                        update(API_REQUESTS)
                        .where(API_REQUESTS.c.uuid == alias_id)
                        .values(run_uuid=None, payload=payload, content_hash=digest(payload))
                    )
                changed = settings.model_copy(update={"stage_timeout_seconds": settings.stage_timeout_seconds + 1})
                owner = RunManager(RunStore(engine, settings.workspace), changed)
                restored = await job_receipt(owner, alias_id)
                assert str(restored.report.report_id) == original["report"]["report_id"]
                replay = await admit_job(owner, alias)
                assert replay.report == restored.report and replay.origin == restored.origin
                assert (await owner.store.row(API_REQUESTS, alias_id))["run_uuid"] == original["run"]["run_id"]
                async with engine.connect() as connection:
                    count = await connection.scalar(
                        select(func.count())
                        .select_from(PREPARATION_ATTEMPTS)
                        .where(PREPARATION_ATTEMPTS.c.workspace == settings.workspace)
                    )
                assert count == 2 and not owner.tasks
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
