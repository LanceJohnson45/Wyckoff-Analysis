"""真实数据入口与PostgreSQL验证API幂等、任务执行和冻结中断恢复。"""

import asyncio
from datetime import date
from pathlib import Path

import httpx
import pytest

from stock_research.config import Settings
from stock_research.errors import ResearchConflict
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.pipeline import prepare_preview
from stock_research.request_store import RequestStore
from stock_research.run_store import RunStore
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_http_api import TOKEN
from tests.stock_research.test_run_store import analyses_for

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


def test_request_claim_recovers_run_created_before_binding(postgres_settings: Settings, snapshot: Snapshot) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "claim-" + new_id())
        requests = RequestStore(store)
        try:
            await initialize_schema(engine)
            body = {"target_session": "2026-11-23"}
            async with requests.claim("synthetic-idempotency-key", body) as request:
                run_id, _ = await prepare_preview(
                    engine,
                    postgres_settings.model_copy(update={"workspace": store.workspace}),
                    snapshot,
                    analyses_for(snapshot),
                    date(2026, 11, 23),
                    request_id=request["uuid"],
                )
                with pytest.raises(ResearchConflict):
                    async with requests.claim("synthetic-idempotency-key", body):
                        pytest.fail("concurrent claim acquired")
            async with requests.claim("synthetic-idempotency-key", body) as recovered:
                assert recovered["run_uuid"] is None
                assert await requests.frozen_run(recovered) == run_id
                await requests.bind(recovered, run_id)
            async with requests.claim("synthetic-idempotency-key", body) as bound:
                assert bound["run_uuid"] == run_id
            with pytest.raises(ResearchConflict, match="载荷"):
                async with requests.claim("synthetic-idempotency-key", {"target_session": "2026-11-24"}):
                    pytest.fail("same key changed request body")
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_http_create_freezes_real_source_and_replays_same_run(
    postgres_settings: Settings, snapshot: Snapshot, market_path: Path, tmp_path: Path
) -> None:
    universe, classifications = tmp_path / "universe.json", tmp_path / "classifications.json"
    universe.write_text(snapshot.universe.model_dump_json())
    classifications.write_text(snapshot.classifications.model_dump_json())
    settings = Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(exclude={"api_token"}),
            "workspace": "actions-" + new_id(),
            "postgres_database": "stock_research_test",
            "artifacts_path": tmp_path / "artifacts",
            "local_market_path": market_path,
            "universe_path": universe,
            "classification_path": classifications,
        },
        api_token=TOKEN,
    )

    async def scenario() -> None:
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            headers = {"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": "fixture-initial-request"}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                body = {"market": "us", "target_session": "2026-11-23", "preview": True}
                created = await client.post("/v1/research-runs", json=body)
                assert created.status_code == 202, created.text
                run_id = created.json()["data"]["run_id"]
                async with asyncio.timeout(10):
                    while (state := await app.state.store.summary(run_id))["status"] in {"pending", "running"}:
                        await asyncio.sleep(0.01)
                assert state["status"] == "completed"
                assert len(state["selections"]) == len(state["reviews"]) == 2
                duplicate = await client.post("/v1/research-runs", json=body)
                assert duplicate.status_code == 202
                assert duplicate.json()["data"]["run_id"] == run_id
                assert (
                    await client.post("/v1/research-runs", json={**body, "target_session": "2026-11-24"})
                ).status_code == 409
                assert (await client.post(f"/v1/research-runs/{run_id}/retry")).status_code == 409
                assert (await client.post(f"/v1/research-runs/{run_id}/cancel")).status_code == 409
                assert (
                    await client.post("/v1/research-runs", json={**body, "snapshot_file": "/etc/passwd"})
                ).status_code == 422
                forced_body = {**body, "force": True, "parent_run_id": run_id}
                forced_headers = {**headers, "Idempotency-Key": "fixture-force-request"}
                forced = await client.post("/v1/research-runs", json=forced_body, headers=forced_headers)
                assert forced.status_code == 202
                forced_id = forced.json()["data"]["run_id"]
                assert forced_id != run_id
                assert (await client.post("/v1/research-runs", json=forced_body, headers=forced_headers)).json()[
                    "data"
                ]["run_id"] == forced_id
            reloaded = RunStore(engine, settings.workspace)
            assert (await reloaded.summary(run_id))["status"] == "completed"
            assert (await reloaded.summary(forced_id))["manifest"]["parent_id"] == run_id
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_manager_cancel_awaits_cleanup_and_persists_cancelled(
    postgres_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """受控挂起仅注入任务延迟；取消结果从真实PG独立查询。"""
    from stock_research.run_manager import RunManager
    from stock_research.schema import RESEARCH_RUNS

    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "cancel-" + new_id())
        manager = RunManager(store, postgres_settings)
        started, cleaned = asyncio.Event(), asyncio.Event()
        try:
            await initialize_schema(engine)
            settings = postgres_settings.model_copy(update={"workspace": store.workspace})
            run_id, _ = await prepare_preview(engine, settings, snapshot, analyses_for(snapshot), date(2026, 11, 23))

            async def controlled(owned_id: str) -> None:
                await store.publish(owned_id, "running")
                started.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cleaned.set()

            monkeypatch.setattr(manager, "run", controlled)
            manager.start(run_id)
            await asyncio.wait_for(started.wait(), 2)
            await manager.cancel(run_id)
            assert cleaned.is_set()
            assert not manager.tasks
            assert (await RunStore(engine, store.workspace).row(RESEARCH_RUNS, run_id))["status"] == "cancelled"
            with pytest.raises(ResearchConflict):
                await manager.cancel(run_id)
        finally:
            await manager.close()
            await close_engine(engine)

    asyncio.run(scenario())


def test_http_unavailable_source_and_non_session_return_explicit_errors(
    postgres_settings: Settings, tmp_path: Path
) -> None:
    async def scenario() -> None:
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token"}),
                "workspace": "unavailable-" + new_id(),
                "postgres_database": "stock_research_test",
                "local_market_path": tmp_path / "missing-market.sqlite3",
            },
            api_token=TOKEN,
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            headers = {"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": "fixture-unavailable-source"}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                unavailable = await client.post("/v1/research-runs", json={"target_session": "2026-11-23"})
                assert unavailable.status_code == 503
                assert unavailable.json()["code"] == "research.unavailable"
                assert str(tmp_path) not in unavailable.text
                holiday = await client.post(
                    "/v1/research-runs",
                    json={"target_session": "2026-11-22"},
                    headers={**headers, "Idempotency-Key": "fixture-invalid-session"},
                )
                assert holiday.status_code == 422
                assert holiday.json()["code"] == "research.invalid_input"
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
