"""真实PG/API/worker参数实验作业，不发布生产参数。"""

import asyncio
import json
import time

import httpx
import pytest

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.schema import EVALUATION_JOBS
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_evaluation_api import OPERATOR
from tests.stock_research.test_evaluation_api import evaluation_settings as evaluation_settings


async def initialize(settings: Settings) -> None:
    engine = database_engine(settings)
    try:
        await initialize_schema(engine)
    finally:
        await close_engine(engine)


async def terminal(client: httpx.AsyncClient, identifier: str) -> dict[str, object]:
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        response = await client.get("/v1/parameter-experiments/" + identifier)
        assert response.status_code == 200
        data = response.json()["data"]
        if data["status"] in {"completed", "failed", "cancelled"}:
            return data
        await asyncio.sleep(0.03)
    raise AssertionError("参数实验未到终态")


async def configuration_files(settings: Settings, identifier: str) -> dict[str, object]:
    """读取持久化配置，验证冻结语义不因API请求默认值丢失。"""
    from sqlalchemy import select

    engine = database_engine(settings)
    try:
        async with engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        select(EVALUATION_JOBS.c.payload).where(
                            EVALUATION_JOBS.c.workspace == settings.workspace,
                            EVALUATION_JOBS.c.uuid == identifier,
                            EVALUATION_JOBS.c.is_deleted.is_(False),
                        )
                    )
                )
                .mappings()
                .one()
            )
        return row["payload"]["configuration"]["files"]
    finally:
        await close_engine(engine)


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_parameter_experiment_api_completes_and_replays_original(evaluation_settings: Settings) -> None:
    """[INTEGRATION] 三个显式变体同输入完成；原键重放不新建，历史评估不混入。"""

    async def scenario() -> None:
        await initialize(evaluation_settings)
        app = create_app(evaluation_settings)
        body = {
            "profile": "acceptance-baseline-v1",
            "cutoff": "2026-11-20",
            "field": "support_atr",
            "values": [0.5, 0.75, 1.0],
        }
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://127.0.0.1:8788",
                headers={"Authorization": "Bearer " + OPERATOR},
            ) as client,
        ):
            accepted = await client.post(
                "/v1/parameter-experiments", json=body, headers={"Idempotency-Key": "parameter-api-000000000001"}
            )
            assert accepted.status_code == 202, accepted.text
            identifier = accepted.json()["data"]["id"]
            completed = await terminal(client, identifier)
            assert completed["status"] == "completed", completed
            assert (await configuration_files(evaluation_settings, identifier))["allow_current_universe"] is True
            assert completed["values"] == [0.5, 0.75, 1.0]
            replay = await client.post(
                "/v1/parameter-experiments", json=body, headers={"Idempotency-Key": "parameter-api-000000000001"}
            )
            assert replay.status_code == 202 and replay.json()["data"] == completed
            changed = await client.post(
                "/v1/parameter-experiments",
                json={**body, "values": [0.6, 0.75, 1.0]},
                headers={"Idempotency-Key": "parameter-api-000000000001"},
            )
            assert changed.status_code == 409
            result = (await client.get("/v1/parameter-experiment-results/" + completed["result_id"])).json()["data"]
            assert len(result["variants"]) == 3 and result["manual_review"] == "pending"
            assert {variant["value"] for variant in result["variants"]} == {0.5, 0.75, 1.0}
            assert len({variant["input_hash"] for variant in result["variants"]}) == 1
            assert str(evaluation_settings.artifacts_path) not in json.dumps(result)
            assert (await client.get("/v1/parameter-experiments")).json()["data"]["total"] == 1
            assert (await client.get("/v1/evaluations")).json()["data"]["total"] == 0
            assert (await client.get("/v1/evaluations/" + identifier)).status_code == 404
            assert (await client.get("/v1/evaluation-results/" + completed["result_id"])).status_code == 404

    asyncio.run(scenario())
