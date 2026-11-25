"""正式初选观测的实际API/PG/worker，历史量化资源保持独立。"""

import asyncio
import os
import signal
import sqlite3
from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr
from sqlalchemy import create_engine, func, select, update

from stock_research.config import Settings
from stock_research.data import DAILY_BARS
from stock_research.evaluation_store import EvaluationStore
from stock_research.http_api import create_app
from stock_research.models import Snapshot
from stock_research.queries import latest_report_id
from stock_research.run_store import RunStore
from stock_research.schema import EVALUATION_JOBS, EVALUATION_RESULTS, EVALUATION_SAMPLES
from stock_research.schemas.selection_observation_execution import SelectionObservationFrame
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_evaluation_api import OPERATOR, READER, WORKFLOW
from tests.stock_research.test_selection_observations import add_selection, seed_report, stored_records
from tests.stock_research.test_selection_observations import selection_settings as selection_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


async def terminal(client: httpx.AsyncClient, identifier: str) -> dict[str, object]:
    """等候实际终态，202或输入文件存在都不算成功。"""
    async with asyncio.timeout(20):
        while True:
            response = await client.get("/v1/selection-observations/" + identifier)
            assert response.status_code == 200
            body = response.json()["data"]
            if body["status"] in {"completed", "failed", "cancelled"}:
                return body
            await asyncio.sleep(0.02)


async def pause_after_freeze(app, monkeypatch: pytest.MonkeyPatch) -> tuple[asyncio.Event, list[int]]:
    """冻结后暂停真实worker，给取消和续跑创造可观察窗口。"""
    ready, pids = asyncio.Event(), []
    original = app.state.evaluations.jobs.observation_progress

    async def pause(identifier: str, frame: SelectionObservationFrame) -> None:
        await original(identifier, frame)
        if frame.status == "frozen":
            os.kill(frame.pid, signal.SIGSTOP)
            pids.append(frame.pid)
            ready.set()

    monkeypatch.setattr(app.state.evaluations.jobs, "observation_progress", pause)
    return ready, pids


def test_idempotent_formal_observation_keeps_original_report_and_historical_resources(
    selection_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【幂等性】真实完成后重放只读原结果，缺数和缺策略保留，旧历史列表不混入。"""

    async def scenario() -> None:
        settings = selection_settings.model_copy(
            update={
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
                "api_workflow_token": SecretStr(WORKFLOW),
                "premarket_source_workspace": "synthetic-formal-observations",
                "model_credentials_path": tmp_path / "no-model-credentials",
            }
        )
        _, report_id = await seed_report(settings, snapshot)
        before = await stored_records(settings)
        body = {"report_id": report_id, "cutoff": "2026-11-24"}
        headers = {"Idempotency-Key": "formal-observation-synthetic-0001"}
        app = create_app(settings)

        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            accepted = await client.post("/v1/selection-observations", json=body, headers=headers)
            assert accepted.status_code == 202, accepted.text
            identifier = accepted.json()["data"]["id"]
            completed = await terminal(client, identifier)
            assert completed["status"] == "completed", completed
            assert completed["kind"] == "formal_selection_observations" and completed["attempt_count"] == 1
            replay = await client.post("/v1/selection-observations", json=body, headers=headers)
            assert replay.status_code == 202 and replay.json()["data"] == completed
            changed = await client.post(
                "/v1/selection-observations", json={**body, "cutoff": "2026-11-23"}, headers=headers
            )
            assert changed.status_code == 409

            result_id = completed["result_id"]
            response = await client.get("/v1/selection-observation-results/" + result_id)
            assert response.status_code == 200
            result = response.json()["data"]
            assert result["scope"]["report_id"] == report_id and result["scope"]["report_status"] == "partial"
            assert result["manual_review"] == "pending" and result["missing_strategies"] == ["wyckoff"]
            samples = (
                await client.get("/v1/selection-observation-results/" + result_id + "/samples?page_size=1")
            ).json()["data"]
            assert samples["total"] == 1 and samples["page_size"] == 1
            sample = samples["items"][0]
            assert sample["strategy"] == "martin" and sample["symbol"] == "AAPL"
            assert sample["reference_session"] == "2026-11-23"
            assert [row["horizon"] for row in sample["windows"]] == [1, 3, 5, 10, 20]
            assert {row["status"] for row in sample["windows"]} == {"missing_data", "pending"}
            assert (await client.get("/v1/selection-observations")).json()["data"]["total"] == 1
            assert (await client.get("/v1/evaluations")).json()["data"]["total"] == 0
            assert (await client.get("/v1/evaluations/" + identifier)).status_code == 404
            assert (await client.get("/v1/evaluation-results/" + result_id)).status_code == 404
            assert str(settings.local_market_path) not in response.text

        assert await stored_records(settings) == before
        engine = database_engine(settings)
        try:
            async with engine.connect() as connection:
                counts = [
                    await connection.scalar(
                        select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                    )
                    for table in (EVALUATION_JOBS, EVALUATION_RESULTS, EVALUATION_SAMPLES)
                ]
            assert counts == [1, 1, 1]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_formal_observation_empty_selection_is_valid_result(
    selection_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【边界】空初选是成功观测，样本分母为0且人工审查仍pending。"""

    async def scenario() -> None:
        settings = selection_settings.model_copy(
            update={
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
                "api_workflow_token": SecretStr(WORKFLOW),
                "premarket_source_workspace": "synthetic-formal-observations",
                "model_credentials_path": tmp_path / "no-model-credentials",
            }
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            store = RunStore(engine, settings.workspace)
            run, _ = await seed_report(settings, snapshot)
            await add_selection(store, run, "wyckoff", empty=True)
            await store.publish(run, "completed")
            report_id = await latest_report_id(store, run_id=run)
        finally:
            await close_engine(engine)
        app = create_app(settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            accepted = await client.post(
                "/v1/selection-observations",
                json={"report_id": report_id, "cutoff": "2026-11-24"},
                headers={"Idempotency-Key": "formal-observation-empty-001"},
            )
            identifier = accepted.json()["data"]["id"]
            completed = await terminal(client, identifier)
            assert completed["status"] == "completed", completed
            result = (await client.get("/v1/selection-observation-results/" + completed["result_id"])).json()["data"]
            assert result["selection_count"] == 2 and result["observation_count"] == 1
            assert result["missing_strategies"] == []
            samples = await client.get("/v1/selection-observation-results/" + completed["result_id"] + "/samples")
            assert samples.json()["data"]["total"] == 1

    asyncio.run(scenario())


def test_formal_observation_permissions_and_validation(
    selection_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【边界】新资源仅operator可用，不接受路径注入、未完成日期或非法分页。"""

    async def scenario() -> None:
        settings = selection_settings.model_copy(
            update={
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
                "api_workflow_token": SecretStr(WORKFLOW),
                "premarket_source_workspace": "synthetic-formal-observations",
                "model_credentials_path": tmp_path / "no-model-credentials",
            }
        )
        _, report_id = await seed_report(settings, snapshot)
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788")
            try:
                assert (await client.get("/v1/selection-observations")).status_code == 401
                for token in (READER, WORKFLOW):
                    response = await client.get(
                        "/v1/selection-observations", headers={"Authorization": "Bearer " + token}
                    )
                    assert response.status_code == 403
                client.headers["Authorization"] = "Bearer " + OPERATOR
                key = {"Idempotency-Key": "formal-observation-boundary-001"}
                for body in (
                    {"report_id": report_id, "cutoff": "2039-01-03"},
                    {"report_id": report_id, "cutoff": "2026-11-24", "market_path": "/etc/passwd"},
                ):
                    response = await client.post("/v1/selection-observations", json=body, headers=key)
                    assert response.status_code == 422
                assert (await client.get("/v1/selection-observations?page_size=101")).status_code == 422
                assert (await client.get("/v1/selection-observations")).json()["data"]["total"] == 0
            finally:
                await client.aclose()

    asyncio.run(scenario())


def test_formal_observation_cancel_retry_and_corrupt_input_rejection(
    selection_settings: Settings,
    snapshot: Snapshot,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """【恢复】取消回收真实worker；续跑只读原输入，损坏冻结文件不得重建。"""

    async def scenario() -> None:
        settings = selection_settings.model_copy(
            update={
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
                "api_workflow_token": SecretStr(WORKFLOW),
                "premarket_source_workspace": "synthetic-formal-observations",
                "model_credentials_path": tmp_path / "no-model-credentials",
            }
        )
        _, report_id = await seed_report(settings, snapshot)
        app = create_app(settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            ready, pids = await pause_after_freeze(app, monkeypatch)
            accepted = await client.post(
                "/v1/selection-observations",
                json={"report_id": report_id, "cutoff": "2026-11-24"},
                headers={"Idempotency-Key": "formal-observation-cancel-001"},
            )
            identifier = accepted.json()["data"]["id"]
            await asyncio.wait_for(ready.wait(), 10)
            frozen = await EvaluationStore(app.state.store).read(identifier)
            input_path = Path(frozen["payload"]["configuration"]["files"]["frozen_input_path"])
            cancelled = await client.post(f"/v1/selection-observations/{identifier}/cancel")
            assert cancelled.status_code == 200
            assert cancelled.json()["data"]["status"] == "cancelled"
            with pytest.raises(ProcessLookupError):
                os.kill(pids[0], 0)
            engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(settings.local_market_path))
            try:
                with engine.begin() as connection:
                    connection.execute(update(DAILY_BARS).values(volume=999))
            finally:
                engine.dispose()
            input_path.write_text("{}\n", encoding="utf-8")
            retry = await client.post(f"/v1/selection-observations/{identifier}/retry")
            assert retry.status_code == 202
            failed = await terminal(client, identifier)
            assert failed["status"] == "failed" and failed["attempt_count"] == 2
            assert failed["error_code"] == "evaluation_calculation_failed"
            assert (await client.post(f"/v1/selection-observations/{identifier}/retry")).status_code == 409

    asyncio.run(scenario())


def test_formal_observation_cancel_finalizes_before_releasing_owner(
    selection_settings: Settings, snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【并发】取消收尾持原生执行锁，其他实例不能抢跑恢复覆盖状态。"""

    async def scenario() -> None:
        from stock_research.errors import ResearchConflict

        settings = selection_settings.model_copy(
            update={
                "api_token": SecretStr(OPERATOR),
                "api_read_token": SecretStr(READER),
                "api_workflow_token": SecretStr(WORKFLOW),
                "premarket_source_workspace": "synthetic-formal-observations",
                "model_credentials_path": tmp_path / "no-model-credentials",
            }
        )
        _, report_id = await seed_report(settings, snapshot)
        app = create_app(settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            ready, pids = await pause_after_freeze(app, monkeypatch)
            finishing, release = asyncio.Event(), asyncio.Event()
            finish = app.state.evaluations.finish_cancel

            async def delay(identifier: str) -> None:
                finishing.set()
                await release.wait()
                await finish(identifier)

            monkeypatch.setattr(app.state.evaluations, "finish_cancel", delay)
            accepted = await client.post(
                "/v1/selection-observations",
                json={"report_id": report_id, "cutoff": "2026-11-24"},
                headers={"Idempotency-Key": "formal-observation-lock-0001"},
            )
            identifier = accepted.json()["data"]["id"]
            await asyncio.wait_for(ready.wait(), 10)
            cancellation = asyncio.create_task(client.post(f"/v1/selection-observations/{identifier}/cancel"))
            try:
                await asyncio.wait_for(finishing.wait(), 10)
                with pytest.raises(ResearchConflict):
                    async with app.state.store.execution_lock(identifier, table=EVALUATION_JOBS):
                        pass
            finally:
                release.set()
                result = await cancellation
                assert result.status_code == 200

    asyncio.run(scenario())
