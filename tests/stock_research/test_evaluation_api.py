"""真实PG/API/worker评估用户路径，不调用外部模型或行情。"""

import asyncio
import json
import os
import signal
import time
from pathlib import Path

import httpx
import pytest

from stock_research.config import Settings
from stock_research.errors import ResearchConflict
from stock_research.evaluation_store import EvaluationStore
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.schema import EVALUATION_JOBS
from stock_research.schemas.evaluation_execution import EvaluationFiles, EvaluationFrame
from stock_research.store import close_engine, database_engine, initialize_schema

OPERATOR = "evaluation-operator-synthetic-000000000001"
READER = "evaluation-read-only-synthetic-0000000001"
WORKFLOW = "evaluation-workflow-synthetic-00000000001"


@pytest.fixture
def evaluation_settings(postgres_settings: Settings, evaluation_files: EvaluationFiles, tmp_path: Path) -> Settings:
    """本地合成评估配置，禁用条件仅为产生非空存储验收样本。"""
    rules = {name: rule.model_copy(update={"enabled": False}) for name, rule in evaluation_files.martin.rules.items()}
    martin = evaluation_files.martin.model_copy(update={"version": "acceptance-baseline-v1", "rules": rules})
    mpath, wpath = tmp_path / "martin.json", tmp_path / "wyckoff.json"
    mpath.write_text(martin.model_dump_json(), encoding="utf-8")
    wpath.write_text(evaluation_files.wyckoff.model_dump_json(), encoding="utf-8")
    return Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(),
            "postgres_database": "stock_research_test",
            "workspace": "evaluation-api-" + new_id(),
            "local_market_path": evaluation_files.market_path,
            "universe_path": evaluation_files.universe_path,
            "classification_path": evaluation_files.classification_path,
            "parameters_path": mpath,
            "wyckoff_parameters_path": wpath,
            "artifacts_path": tmp_path / "artifacts",
            "api_token": OPERATOR,
            "api_read_token": READER,
            "api_workflow_token": WORKFLOW,
            "premarket_source_workspace": "acceptance",
        },
    )


async def initialize(settings: Settings) -> None:
    """仅初始化fixture临时原生PG，服务启动不承担迁移。"""
    engine = database_engine(settings)
    try:
        await initialize_schema(engine)
    finally:
        await close_engine(engine)


async def terminal(client: httpx.AsyncClient, identifier: str) -> dict[str, object]:
    """有界轮询实际终态，不能把受理成功算成完成。"""
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        response = await client.get("/v1/evaluations/" + identifier)
        assert response.status_code == 200
        data = response.json()["data"]
        if data["status"] in {"completed", "failed", "cancelled"}:
            return data
        await asyncio.sleep(0.03)
    raise AssertionError("评估未到终态")


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_api_create_read_replay_and_samples(evaluation_settings: Settings) -> None:
    """[INTEGRATION] 显式发起到真实完成、私有结果分页和原请求重放。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        body = {"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"}
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            response = await client.post(
                "/v1/evaluations", json=body, headers={"Idempotency-Key": "api-evaluation-000001"}
            )
            assert response.status_code == 202, response.text
            identifier = response.json()["data"]["id"]
            result = await terminal(client, identifier)
            assert result["status"] == "completed", result
            assert result["completed_sections"] == 20 and result["attempt_count"] == 1
            stored = await EvaluationStore(app.state.store).read(identifier)
            with pytest.raises(ProcessLookupError):
                os.kill(stored["payload"]["worker_pid"], 0)
            detail = await client.get("/v1/evaluation-results/" + result["result_id"])
            assert detail.status_code == 200
            data = detail.json()["data"]
            assert len(data["sections"]) == 40 and data["model_selection"] is None
            assert "configuration" not in data and str(evaluation_settings.artifacts_path) not in json.dumps(data)
            samples = await client.get(
                f"/v1/evaluation-results/{result['result_id']}/samples?page_size=5&strategy=martin"
            )
            assert samples.status_code == 200
            assert samples.json()["data"]["total"] == 20 and len(samples.json()["data"]["items"]) == 5
            evaluation_settings.parameters_path.unlink()
            replay = await client.post(
                "/v1/evaluations", json=body, headers={"Idempotency-Key": "api-evaluation-000001"}
            )
            assert replay.status_code == 202 and replay.json()["data"]["id"] == identifier
            conflict = await client.post(
                "/v1/evaluations",
                json={**body, "cutoff": "2026-11-19"},
                headers={"Idempotency-Key": "api-evaluation-000001"},
            )
            assert conflict.status_code == 409
            page = await client.get("/v1/evaluations?page_size=1")
            assert page.json()["data"]["total"] == 1

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_validation_evaluation_api_permissions_and_boundaries(evaluation_settings: Settings) -> None:
    """[VALIDATION] 私有读、盘前身份、路径注入及日期/分页边界。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
            ) as client,
        ):
            assert (await client.get("/v1/evaluations")).status_code == 401
            for token in (READER, WORKFLOW):
                assert (
                    await client.get("/v1/evaluations", headers={"Authorization": "Bearer " + token})
                ).status_code == 403
            client.headers["Authorization"] = "Bearer " + OPERATOR
            key = {"Idempotency-Key": "invalid-evaluation-0001"}
            for body in (
                {"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20", "market_path": "/etc/passwd"},
                {"profile": "acceptance-baseline-v1", "cutoff": "2026-11-21"},
                {"profile": "acceptance-baseline-v1", "cutoff": "2039-01-03"},
            ):
                assert (await client.post("/v1/evaluations", json=body, headers=key)).status_code == 422
            assert (await client.get("/v1/evaluations?page_size=101")).status_code == 422
            assert (await client.get("/v1/evaluations/" + new_id())).status_code == 404
            assert (await client.get("/v1/evaluation-results/" + new_id())).status_code == 404
            assert (await client.get("/v1/evaluations")).json()["data"]["total"] == 0

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_cancel_and_retry_keep_original_input(
    evaluation_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 真正停止冻结后的worker，再用同输入/身份显式续跑。"""
    from sqlalchemy import create_engine, update

    from stock_research.data import DAILY_BARS

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            ready, pids = asyncio.Event(), []
            original = app.state.evaluations.jobs.progress

            async def pause(identifier: str, frame: EvaluationFrame) -> None:
                await original(identifier, frame)
                if frame.status == "frozen":
                    os.kill(frame.pid, signal.SIGSTOP)
                    pids.append(frame.pid)
                    ready.set()

            with monkeypatch.context() as control:
                control.setattr(app.state.evaluations.jobs, "progress", pause)
                response = await client.post(
                    "/v1/evaluations",
                    json={"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"},
                    headers={"Idempotency-Key": "cancel-evaluation-000001"},
                )
                identifier = response.json()["data"]["id"]
                await asyncio.wait_for(ready.wait(), 10)
                frozen = await EvaluationStore(app.state.store).read(identifier)
                fingerprint = frozen["payload"]["configuration"]["files"]["expected_input_hash"]
                cancelled = await client.post(f"/v1/evaluations/{identifier}/cancel")
                assert cancelled.status_code == 200 and cancelled.json()["data"]["status"] == "cancelled"
                with pytest.raises(ProcessLookupError):
                    os.kill(pids[0], 0)
            engine = create_engine("sqlite:///" + str(evaluation_settings.local_market_path))
            try:
                with engine.begin() as connection:
                    connection.execute(update(DAILY_BARS).values(volume=999))
            finally:
                engine.dispose()
            retried = await client.post(f"/v1/evaluations/{identifier}/retry")
            assert retried.status_code == 202
            completed = await terminal(client, identifier)
            assert completed["status"] == "completed" and completed["attempt_count"] == 2, completed
            result = (await client.get("/v1/evaluation-results/" + completed["result_id"])).json()["data"]
            assert result["input_hash"] == fingerprint
            assert (await client.post(f"/v1/evaluations/{identifier}/retry")).status_code == 409

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_restart_uses_frozen_versions(
    evaluation_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 服务关闭真实回收worker，原operator文件移走仍可按冻结清单恢复。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            ready, pids = asyncio.Event(), []
            original = app.state.evaluations.jobs.progress

            async def pause(identifier: str, frame: EvaluationFrame) -> None:
                await original(identifier, frame)
                if frame.status == "frozen":
                    os.kill(frame.pid, signal.SIGSTOP)
                    pids.append(frame.pid)
                    ready.set()

            with monkeypatch.context() as control:
                control.setattr(app.state.evaluations.jobs, "progress", pause)
                response = await client.post(
                    "/v1/evaluations",
                    json={"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"},
                    headers={"Idempotency-Key": "restart-evaluation-0001"},
                )
                identifier = response.json()["data"]["id"]
                await asyncio.wait_for(ready.wait(), 10)
        with pytest.raises(ProcessLookupError):
            os.kill(pids[0], 0)
        for path in (
            evaluation_settings.parameters_path,
            evaluation_settings.universe_path,
            evaluation_settings.classification_path,
        ):
            path.unlink()
        restored = create_app(evaluation_settings)
        async with (
            restored.router.lifespan_context(restored),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=restored),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            completed = await terminal(client, identifier)
            assert completed["status"] == "completed" and completed["attempt_count"] == 2, completed

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_interrupted_worker_recovers_same_job(
    evaluation_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 首次真实SIGTERM后默认入口有界恢复，同一任务仅两次尝试。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            original = app.state.evaluations.jobs.progress
            interrupted = []

            async def interrupt(identifier: str, frame: EvaluationFrame) -> None:
                await original(identifier, frame)
                if frame.status == "frozen" and not interrupted:
                    interrupted.append(frame.pid)
                    os.kill(frame.pid, signal.SIGTERM)

            monkeypatch.setattr(app.state.evaluations.jobs, "progress", interrupt)
            response = await client.post(
                "/v1/evaluations",
                json={"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"},
                headers={"Idempotency-Key": "interrupt-evaluation-0001"},
            )
            identifier = response.json()["data"]["id"]

            completed = await terminal(client, identifier)

            assert completed["status"] == "completed" and completed["attempt_count"] == 2, completed
            with pytest.raises(ProcessLookupError):
                os.kill(interrupted[0], 0)
            assert (await client.get("/v1/evaluations")).json()["data"]["total"] == 1

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_cancel_finalizes_before_releasing_owner(
    evaluation_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 取消收尾仍持原生执行锁，另一实例不能抢先恢复并被旧拥有者覆盖。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            frozen, finishing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
            progress, finish = app.state.evaluations.jobs.progress, app.state.evaluations.finish_cancel

            async def pause(identifier: str, frame: EvaluationFrame) -> None:
                await progress(identifier, frame)
                if frame.status == "frozen":
                    os.kill(frame.pid, signal.SIGSTOP)
                    frozen.set()

            async def delay_finish(identifier: str) -> None:
                finishing.set()
                await release.wait()
                await finish(identifier)

            monkeypatch.setattr(app.state.evaluations.jobs, "progress", pause)
            monkeypatch.setattr(app.state.evaluations, "finish_cancel", delay_finish)
            response = await client.post(
                "/v1/evaluations",
                json={"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"},
                headers={"Idempotency-Key": "owner-release-evaluation-001"},
            )
            identifier = response.json()["data"]["id"]
            await asyncio.wait_for(frozen.wait(), 10)
            cancellation = asyncio.create_task(client.post(f"/v1/evaluations/{identifier}/cancel"))
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


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_evaluation_missing_configuration_finishes_failed(evaluation_settings: Settings) -> None:
    """[INTEGRATION] 准备所需文件缺失应落失败终态，不遗留永久pending。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        evaluation_settings.parameters_path.unlink()
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            response = await client.post(
                "/v1/evaluations",
                json={"profile": "acceptance-baseline-v1", "cutoff": "2026-11-20"},
                headers={"Idempotency-Key": "missing-configuration-0001"},
            )
            identifier = response.json()["data"]["id"]
            await asyncio.wait_for(
                asyncio.gather(*tuple(app.state.evaluations.tasks.values()), return_exceptions=True), 10
            )

            data = (await client.get("/v1/evaluations/" + identifier)).json()["data"]

            assert data["status"] == "failed" and data["error_code"] == "evaluation_prepare_failed", data
            assert data["attempt_count"] == 0 and data["result_id"] is None

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
@pytest.mark.parametrize(
    ("cutoff", "remove_stock", "expected_code"),
    [
        pytest.param("2026-11-18", False, "evaluation_history_insufficient", id="insufficient-history"),
        pytest.param("2026-11-20", True, "evaluation_quality_failed", id="snapshot-quality"),
    ],
)
def test_integration_evaluation_data_failure_requires_new_input(
    evaluation_settings: Settings, cutoff: str, remove_stock: bool, expected_code: str
) -> None:
    """[INTEGRATION] 真实资料失败明确归因，不浪费剩余次数重算同一份坏输入。"""
    from sqlalchemy import create_engine, delete

    from stock_research.data import DAILY_BARS

    async def scenario() -> None:
        await initialize(evaluation_settings)
        if remove_stock:
            engine = create_engine("sqlite:///" + str(evaluation_settings.local_market_path))
            try:
                with engine.begin() as connection:
                    connection.execute(delete(DAILY_BARS).where(DAILY_BARS.c.kind == "stock"))
            finally:
                engine.dispose()
        app = create_app(evaluation_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            response = await client.post(
                "/v1/evaluations",
                json={"profile": "acceptance-baseline-v1", "cutoff": cutoff},
                headers={"Idempotency-Key": "data-prerequisite-000001"},
            )
            assert response.status_code == 202
            identifier = response.json()["data"]["id"]
            result = await terminal(client, identifier)
            assert result["status"] == "failed" and result["error_code"] == expected_code, result
            assert result["attempt_count"] == 1 and result["result_id"] is None
            original = await EvaluationStore(app.state.store).read(identifier)
            retry = await client.post(f"/v1/evaluations/{identifier}/retry")
            assert retry.status_code == 409 and retry.json()["code"] == "research.conflict"
            assert (await EvaluationStore(app.state.store).read(identifier))["payload"] == original["payload"]

    asyncio.run(scenario())
