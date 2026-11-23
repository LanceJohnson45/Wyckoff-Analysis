"""真实PG/行情文件验证人工确认、CAS与API/CLI消费；仅控制合成时钟。"""

import asyncio
from datetime import date, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select, update

from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchInputError
from stock_research.http_api import create_app
from stock_research.identity import SHANGHAI, new_id
from stock_research.input_configuration import apply_input_publications, input_baseline, resolve_run_inputs
from stock_research.input_maintenance import InputMaintenance
from stock_research.input_records import publications_for_date
from stock_research.models import ClassificationVersion, Snapshot, UniverseVersion
from stock_research.pipeline import run_preview
from stock_research.queries import latest_report_id
from stock_research.research_commands import run_command
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS, INPUT_PUBLICATIONS, RESEARCH_REPORTS
from stock_research.schemas.input_maintenance import ConfirmTheme, CreateManualOverride, ThemeDefinition
from stock_research.store import close_engine, database_engine, initialize_schema, load_snapshot
from tests.stock_research.test_queries import OPERATOR, READER
from tests.stock_research.test_run_store import analyses_for


@pytest.fixture
def input_settings(
    postgres_settings: Settings, snapshot: Snapshot, market_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Settings:
    universe, classifications = tmp_path / "universe.json", tmp_path / "classifications.json"
    universe.write_text(snapshot.universe.model_dump_json())
    classifications.write_text(snapshot.classifications.model_dump_json())
    monkeypatch.setattr("stock_research.input_maintenance.now", lambda: datetime(2026, 11, 19, 12, tzinfo=SHANGHAI))
    return Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(exclude={"api_token", "api_read_token"}),
            "workspace": "inputs-" + new_id(),
            "postgres_database": "stock_research_test",
            "artifacts_path": tmp_path / "artifacts",
            "local_market_path": market_path,
            "universe_path": universe,
            "classification_path": classifications,
        },
        api_token=OPERATOR,
        api_read_token=READER,
    )


def test_validation_manual_input_contract() -> None:
    """【参数校验】成员别名去重、Unicode/NUL及动作资料边界。"""
    theme = ThemeDefinition(name=" Devices ", members=("BRK.B", "AAPL"), source="fixture", reason="fixture")
    assert theme.name == "Devices" and theme.members == ("AAPL", "BRK-B")
    for body in (
        {"members": ("BRK.B", "BRK-B")},
        {"name": " "},
        {"source": "bad\x00source"},
        {"members": ("../.env",)},
    ):
        with pytest.raises(ValueError):
            ThemeDefinition.model_validate({**theme.model_dump(), **body})
    with pytest.raises(ValueError):
        CreateManualOverride(
            symbol="NEW",
            action="add",
            source="fixture",
            reason="fixture",
            expected_revision=0,
            effective_date=date(2026, 11, 20),
        )


@pytest.mark.usefixtures("local_postgres_socket")
def test_idempotent_theme_confirmation_and_native_cas(input_settings: Settings) -> None:
    """【并发/幂等】建议不生效，同目标两提议仅一胜出，重放不追加修订。"""

    async def scenario() -> None:
        engine = database_engine(input_settings)
        store = RunStore(engine, input_settings.workspace)
        service = InputMaintenance(store, input_settings)
        try:
            await initialize_schema(engine)
            definition = ThemeDefinition(name="Confirmed", members=("AAPL",), source="synthetic", reason="人工合成验收")
            first = await service.propose("fixture-theme-first", definition)
            second = await service.propose(
                "fixture-theme-second", definition.model_copy(update={"reason": "另一个合成提议"})
            )
            assert first.status == "pending" and first.current_revision == 0
            assert not await publications_for_date(store, date(2026, 11, 20))
            request = ConfirmTheme(expected_revision=0, effective_date=date(2026, 11, 20))
            outcomes = await asyncio.gather(
                service.confirm(str(first.id), request),
                service.confirm(str(second.id), request),
                return_exceptions=True,
            )
            assert sum(isinstance(item, ResearchConflict) for item in outcomes) == 1
            publication = next(item for item in outcomes if not isinstance(item, Exception))
            winner = await service.proposal(str(publication.proposal_id))
            assert winner.status == "confirmed" and winner.current_revision == 1
            assert (await service.confirm(str(winner.id), request)).id == publication.id
            assert (await service.propose("fixture-theme-first", definition)).id == first.id
            with pytest.raises(ResearchConflict):
                await service.confirm(str(winner.id), request.model_copy(update={"effective_date": date(2026, 11, 21)}))
            with pytest.raises(ResearchConflict):
                await service.propose("fixture-theme-first", definition.model_copy(update={"members": ()}))
            missing = await service.propose(
                "fixture-theme-missing", definition.model_copy(update={"name": "Missing", "members": ("ZZZZ",)})
            )
            with pytest.raises(ResearchInputError):
                await service.confirm(str(missing.id), request)
            with pytest.raises(ResearchInputError):
                await service.confirm(
                    str(missing.id), request.model_copy(update={"effective_date": date(2026, 11, 17)})
                )
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(INPUT_PUBLICATIONS)
                        .where(INPUT_PUBLICATIONS.c.workspace == store.workspace)
                    )
                    == 1
                )
            resolved, sources = await resolve_run_inputs(store, input_settings, date(2026, 11, 20))
            assert sources.source == "published" and len(sources.publications) == 1
            assert (
                "Confirmed"
                in ClassificationVersion.model_validate_json(resolved.classification_path.read_text()).records[0].themes
            )
            unchanged, old_sources = await resolve_run_inputs(store, input_settings, date(2026, 11, 18))
            assert unchanged.classification_path.read_bytes() != b"" and not old_sources.publications
            assert (
                ClassificationVersion.model_validate_json(unchanged.classification_path.read_text())
                == (await input_baseline(input_settings))[1]
            )
            foreign = InputMaintenance(RunStore(engine, "foreign"), input_settings)
            with pytest.raises(ValueError):
                await foreign.proposal(str(first.id))
            clearing = await service.propose("fixture-theme-clear", definition.model_copy(update={"members": ()}))
            await service.confirm(
                str(clearing.id), ConfirmTheme(expected_revision=1, effective_date=date(2026, 11, 21))
            )
            cleared, later = await resolve_run_inputs(store, input_settings, date(2026, 11, 21))
            assert later.publications[0].revision == 2
            assert (
                "Confirmed"
                not in ClassificationVersion.model_validate_json(cleared.classification_path.read_text())
                .records[0]
                .themes
            )
            assert (await resolve_run_inputs(store, input_settings, date(2026, 11, 20)))[1] == sources
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_manual_removal_preserves_indices_and_future_versions(input_settings: Settings) -> None:
    """【正常/边界】缺数自选保留，移除manual不删指数；异参同键并发安全。"""

    async def scenario() -> None:
        engine = database_engine(input_settings)
        store = RunStore(engine, input_settings.workspace)
        service = InputMaintenance(store, input_settings)
        try:
            await initialize_schema(engine)
            body = CreateManualOverride(
                symbol="AAPL",
                action="add",
                name="Human name",
                industry="Never override industry",
                source="synthetic",
                reason="合成自选",
                expected_revision=0,
                effective_date=date(2026, 11, 20),
            )
            added = await service.override("fixture-manual-add", body)
            assert (await service.override("fixture-manual-add", body)).id == added.id
            versions = apply_input_publications(
                *(await input_baseline(input_settings)),
                await publications_for_date(store, date(2026, 11, 20)),
                date(2026, 11, 20),
            )
            assert {item.index for item in versions[0].active(date(2026, 11, 20))["AAPL"]} == {
                "sp500",
                "nasdaq100",
                "manual",
            }
            assert versions[1].records[0].industry == "Hardware"
            base_universe, base_classifications = await input_baseline(input_settings)
            future_record = base_classifications.records[0].model_copy(
                update={
                    "company_name": "Future name",
                    "industry": None,
                    "themes": ("Future theme",),
                    "effective_date": date(2026, 11, 21),
                }
            )
            _, current_classifications = apply_input_publications(
                base_universe,
                base_classifications.model_copy(update={"records": (future_record,)}),
                (added,),
                date(2026, 11, 20),
            )
            assert current_classifications.records[0].company_name == "Human name"
            assert not current_classifications.records[0].themes
            remove = CreateManualOverride(
                symbol="AAPL",
                action="remove",
                source="synthetic",
                reason="移除人工关系",
                expected_revision=1,
                effective_date=date(2026, 11, 20),
            )
            await service.override("fixture-manual-remove", remove)
            before_future = await publications_for_date(store, date(2026, 11, 20))
            future = body.model_copy(update={"expected_revision": 2, "effective_date": date(2026, 11, 21)})
            await service.override("fixture-manual-future", future)
            assert await publications_for_date(store, date(2026, 11, 20)) == before_future
            versions = apply_input_publications(
                *(await input_baseline(input_settings)), before_future, date(2026, 11, 20)
            )
            assert {item.index for item in versions[0].active(date(2026, 11, 20))["AAPL"]} == {"sp500", "nasdaq100"}
            different = body.model_copy(update={"symbol": "NEW", "name": "New fixture", "industry": None})
            other = different.model_copy(update={"symbol": "OTHER"})
            outcomes = await asyncio.gather(
                service.override("fixture-same-key-different", different),
                service.override("fixture-same-key-different", other),
                return_exceptions=True,
            )
            assert sum(isinstance(item, ResearchConflict) for item in outcomes) == 1
            resolved, _sources = await resolve_run_inputs(store, input_settings, date(2026, 11, 20))
            universe = UniverseVersion.model_validate_json(resolved.universe_path.read_text())
            assert len(universe.active(date(2026, 11, 20))) == 2
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_http_and_cli_consume_confirmed_inputs_and_keep_original_snapshot(
    input_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【生产接线】真实API建run及CLI默认入口读同版快照/量化；原文件分支不套当前主题。"""

    async def scenario() -> None:
        engine = database_engine(input_settings)
        store = RunStore(engine, input_settings.workspace)
        try:
            await initialize_schema(engine)
            await run_preview(engine, input_settings, snapshot, analyses_for(snapshot), date(2026, 11, 23))
            original_id = await latest_report_id(store)
            original = await store.row(RESEARCH_REPORTS, original_id)
            app = create_app(input_settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + OPERATOR},
                ) as client,
            ):
                definition = {
                    "name": "Confirmed",
                    "members": ["AAPL"],
                    "source": "synthetic",
                    "reason": "合成API人工确认",
                }
                response = await client.post(
                    "/v1/theme-proposals", json=definition, headers={"Idempotency-Key": "fixture-http-proposal"}
                )
                assert response.status_code == 201, response.text
                proposal = response.json()["data"]
                confirmed = await client.post(
                    f"/v1/theme-proposals/{proposal['id']}/confirm",
                    json={"expected_revision": 0, "effective_date": "2026-11-20"},
                )
                assert confirmed.status_code == 200, confirmed.text
                manual = await client.post(
                    "/v1/universe-overrides",
                    json={
                        "symbol": "AAPL",
                        "action": "add",
                        "name": "Apple",
                        "source": "synthetic",
                        "reason": "合成人工观察",
                        "expected_revision": 0,
                        "effective_date": "2026-11-20",
                    },
                    headers={"Idempotency-Key": "fixture-http-manual"},
                )
                assert manual.status_code == 201, manual.text
                created = await client.post(
                    "/v1/research-runs",
                    json={"target_session": "2026-11-23"},
                    headers={"Idempotency-Key": "fixture-http-published-inputs"},
                )
                assert created.status_code == 202, created.text
                run_id = created.json()["data"]["run_id"]
                async with asyncio.timeout(10):
                    while (body := await store.summary(run_id))["status"] in {"pending", "running"}:
                        await asyncio.sleep(0.01)
                assert body["status"] == "completed"
                frozen = (await client.get(f"/v1/research-runs/{run_id}/inputs")).json()["data"]["inputs"]
                assert frozen["source"] == "published" and len(frozen["publications"]) == 2
                loaded = await load_snapshot(engine, store.workspace, body["manifest"]["snapshot_id"])
                assert "Confirmed" in loaded.classifications.records[0].themes
                assert len(loaded.universe.active(snapshot.as_of)["AAPL"]) == 3
                assert "theme:Confirmed" in {row["name"] for row in analyses_for(loaded)["martin"].groups}
                stored_factors = await store.row(FACTOR_RESULTS, body["manifest"]["factor_results"]["martin"])
                assert "theme:Confirmed" in {row["name"] for row in stored_factors["payload"]["groups"]}
                cli = await run_command(input_settings, None, date(2026, 11, 23))
                assert cli["run_id"] == run_id
                path = tmp_path / "original-snapshot.json"
                path.write_text(snapshot.model_dump_json())
                supplied = await run_command(input_settings, path, date(2026, 11, 23))
                supplied_sources = (await client.get(f"/v1/research-runs/{supplied['run_id']}/inputs")).json()["data"][
                    "inputs"
                ]
                assert supplied_sources["source"] == "frozen_snapshot" and not supplied_sources["publications"]
                assert supplied["manifest"]["snapshot_hash"] == snapshot.content_hash
                for endpoint in (
                    "/v1/theme-proposals",
                    "/v1/input-publications",
                    "/v1/universe-overrides",
                    f"/v1/research-runs/{run_id}/inputs",
                ):
                    assert (
                        await client.get(endpoint, headers={"Authorization": "Bearer " + READER})
                    ).status_code == 403
                    assert (await client.get(endpoint, headers={"Authorization": ""})).status_code == 401
                assert (await client.get("/v1/theme-proposals?status=confirmed&page_size=1")).json()["data"][
                    "total"
                ] == 1
                assert (await client.get("/v1/theme-proposals?status=pending")).json()["data"]["total"] == 0
                assert (await client.get("/v1/universe-overrides/AAPL")).json()["data"]["revision"] == 1
                assert await store.row(RESEARCH_REPORTS, original_id) == original
                async with engine.begin() as connection:
                    await connection.execute(
                        update(INPUT_PUBLICATIONS)
                        .where(INPUT_PUBLICATIONS.c.workspace == store.workspace)
                        .values(content_hash="f" * 64)
                    )
                assert (await client.get("/v1/input-publications")).status_code == 503
                assert (await client.get(f"/v1/reports/{original_id}")).status_code == 200
                assert (await client.get(f"/v1/research-runs/{run_id}/inputs")).status_code == 200
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_error_baseline_drift_requires_new_proposal(input_settings: Settings) -> None:
    """【异常值】没有新发布也须检测operator基线变化，原提议不能静默改解释。"""

    async def scenario() -> None:
        engine = database_engine(input_settings)
        service = InputMaintenance(RunStore(engine, input_settings.workspace), input_settings)
        try:
            await initialize_schema(engine)
            definition = ThemeDefinition(name="Drift", members=("AAPL",), source="synthetic", reason="基线变化验收")
            proposed = await service.propose("fixture-baseline-drift", definition)
            classifications = ClassificationVersion.model_validate_json(input_settings.classification_path.read_text())
            input_settings.classification_path.write_text(
                classifications.model_copy(update={"version": "changed-baseline"}).model_dump_json()
            )
            with pytest.raises(ResearchConflict):
                await service.confirm(
                    str(proposed.id), ConfirmTheme(expected_revision=0, effective_date=date(2026, 11, 20))
                )
            assert (await service.propose("fixture-baseline-drift", definition)).id == proposed.id
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        select(func.count())
                        .select_from(INPUT_PUBLICATIONS)
                        .where(INPUT_PUBLICATIONS.c.workspace == service.store.workspace)
                    )
                    == 0
                )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
