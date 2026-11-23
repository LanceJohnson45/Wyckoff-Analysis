"""参数注册/发布真实PG事务与实际研究消费链，不以写入回执代替接线。"""

import asyncio
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select, update

from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameter_config import frozen_parameters
from stock_research.parameter_store import activate_parameters, published_parameters, register_parameters
from stock_research.parameters import MartinParameters
from stock_research.research_commands import run_command
from stock_research.run_store import RunStore
from stock_research.schema import PARAMETER_ACTIVATIONS, PARAMETER_SETS
from stock_research.schemas.parameters import ActivateParameters, CreateParameterSet
from stock_research.store import close_engine, database_engine, initialize_schema
from stock_research.wyckoff import WyckoffParameters
from tests.stock_research.test_http_api import TOKEN


@pytest.mark.parametrize(
    "strategy,parameters",
    [
        ("martin", {"unexpected": 1}),
        ("martin", {"ema_periods": ["9", 21, 50]}),
        ("martin", {"rules": {"trend": {"enabled": "false", "action": "exclude", "concept_source": "fixture"}}}),
        ("martin", {"algorithm": "production-us-l1-l4-v1"}),
        ("wyckoff", {"overrides": {"min_avg_amount_wan": 10.0}}),
        ("wyckoff", {"version": "x" * 257}),
    ],
)
def test_parameter_requests_reject_wrong_types_or_unsupported_rules(strategy, parameters) -> None:
    with pytest.raises(ValidationError):
        CreateParameterSet.model_validate({"strategy": strategy, "parameters": parameters})
    with pytest.raises(ValidationError):
        ActivateParameters(parameter_set_id=new_id(), expected_revision=True)


@pytest.mark.usefixtures("local_postgres_socket")
def test_native_parameter_versions_cas_and_immutable_registration(postgres_settings: Settings) -> None:
    async def scenario() -> None:
        engine = database_engine(postgres_settings, test=True)
        store = RunStore(engine, "parameters-" + new_id())
        try:
            await initialize_schema(engine)
            body = CreateParameterSet(strategy="martin", parameters=MartinParameters(version="fixture-a"))
            first = await register_parameters(store, body)
            assert (await register_parameters(store, body)).id == first.id
            with pytest.raises(ResearchConflict, match="版本名"):
                await register_parameters(
                    store,
                    CreateParameterSet(
                        strategy="martin", parameters=MartinParameters(version="fixture-a", support_atr=0.5)
                    ),
                )
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(PARAMETER_SETS)
                        .where(PARAMETER_SETS.c.workspace == store.workspace)
                    )
                    == 1
                )
            second = await register_parameters(
                store,
                CreateParameterSet(
                    strategy="martin", parameters=MartinParameters(version="fixture-b", support_atr=0.5)
                ),
            )
            results = await asyncio.gather(
                activate_parameters(store, "martin", str(first.id), 0),
                activate_parameters(store, "martin", str(second.id), 0),
                return_exceptions=True,
            )
            assert sum(isinstance(result, ResearchConflict) for result in results) == 1
            winner = (await published_parameters(store))["martin"]
            assert winner.revision == 1
            with pytest.raises(ResearchConflict, match="修订"):
                await activate_parameters(store, "martin", str(second.id), 0)
            updated = await activate_parameters(store, "martin", str(second.id), 1)
            assert updated.revision == 2 and updated.parameters.support_atr == 0.5
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(PARAMETER_ACTIVATIONS)
                        .where(PARAMETER_ACTIVATIONS.c.workspace == store.workspace)
                    )
                    == 2
                )
            with pytest.raises(ResearchNotFound):
                await activate_parameters(RunStore(engine, "foreign"), "martin", str(first.id), 0)
            with pytest.raises(ResearchConflict, match="策略"):
                await activate_parameters(store, "wyckoff", str(first.id), 0)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_parameter_api_and_cli_consume_published_values_without_rewriting_old_runs(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path: Path, market_path: Path
) -> None:
    async def scenario() -> None:
        parameters, wyckoff, saved = tmp_path / "martin.json", tmp_path / "wyckoff.json", tmp_path / "snapshot.json"
        parameters.write_text(MartinParameters().model_dump_json())
        wyckoff.write_text(WyckoffParameters().model_dump_json())
        saved.write_text(snapshot.model_dump_json())
        universe, classifications = tmp_path / "universe.json", tmp_path / "classifications.json"
        universe.write_text(snapshot.universe.model_dump_json())
        classifications.write_text(snapshot.classifications.model_dump_json())
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token", "api_read_token"}),
                "workspace": "parameter-http-" + new_id(),
                "postgres_database": "stock_research_test",
                "artifacts_path": tmp_path / "artifacts",
                "parameters_path": parameters,
                "wyckoff_parameters_path": wyckoff,
                "universe_path": universe,
                "classification_path": classifications,
                "local_market_path": market_path,
            },
            api_token=TOKEN,
            api_read_token="synthetic-parameter-reader-with-32-characters",
        )
        engine = database_engine(settings)
        store = RunStore(engine, settings.workspace)
        try:
            await initialize_schema(engine)
            original = await run_command(settings, saved, snapshot.as_of.replace(day=23))
            original_parameters = await frozen_parameters(store, original["run_id"])
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": f"Bearer {TOKEN}"},
                ) as client,
            ):
                before = (await client.get("/v1/strategies")).json()["data"]["items"]
                assert before[0]["source"] == "operator_baseline" and before[0]["revision"] == 0
                created = await client.post(
                    "/v1/parameter-sets",
                    json={
                        "strategy": "martin",
                        "parameters": {"version": "fixture-http-v2", "ema_periods": [5, 10, 20]},
                    },
                )
                assert created.status_code == 201
                parameter_id = created.json()["data"]["id"]
                page = (await client.get("/v1/parameter-sets?page_size=1")).json()["data"]
                assert page["total"] == 1 and len(page["items"]) == 1
                request = {"parameter_set_id": parameter_id, "expected_revision": 0}
                assert (await client.post("/v1/strategies/martin/activate-parameters", json=request)).status_code == 200
                assert (await client.post("/v1/strategies/martin/activate-parameters", json=request)).status_code == 409
                readonly = {"Authorization": "Bearer synthetic-parameter-reader-with-32-characters"}
                assert (await client.get("/v1/strategies", headers=readonly)).status_code == 200
                assert (await client.post("/v1/parameter-sets", headers=readonly, json={})).status_code == 403
                assert (
                    await client.post("/v1/strategies/martin/activate-parameters", headers=readonly, json=request)
                ).status_code == 403
                assert (
                    await client.post(
                        "/v1/parameter-sets", json={"strategy": "martin", "parameters": {"ema_periods": ["9", 21, 50]}}
                    )
                ).status_code == 422
                parameters.write_text(MartinParameters(version="changed-operator-file").model_dump_json())
                next_run = await run_command(settings, saved, snapshot.as_of.replace(day=23))
                assert next_run["run_id"] != original["run_id"] and next_run["status"] == "completed"
                current = (await client.get(f"/v1/research-runs/{next_run['run_id']}/parameters")).json()["data"][
                    "items"
                ]
                assert current[0]["parameters"]["ema_periods"] == [5, 10, 20]
                assert current[0]["parameter_set_id"] == parameter_id and current[0]["revision"] == 1
                accepted = await client.post(
                    "/v1/research-runs",
                    headers={"Idempotency-Key": "synthetic-published-http-run"},
                    json={"market": "us", "target_session": "2026-11-23", "preview": True},
                )
                assert accepted.status_code == 202 and accepted.json()["data"]["run_id"] == next_run["run_id"]
                assert await frozen_parameters(store, original["run_id"]) == original_parameters
                assert (await store.summary(original["run_id"]))["selections"] == original["selections"]
                async with engine.begin() as connection:
                    await connection.execute(
                        update(PARAMETER_SETS)
                        .where(PARAMETER_SETS.c.uuid == parameter_id)
                        .values(content_hash="0" * 64)
                    )
                assert (await client.get("/v1/strategies")).status_code == 503
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
