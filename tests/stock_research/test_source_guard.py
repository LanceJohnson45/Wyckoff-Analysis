"""源码覆盖、缓存接线和实际API动作门控，不修改真实仓库源码。"""

import asyncio
from pathlib import Path

import httpx
import pytest
from sqlalchemy import func, select

from stock_research import research_commands, source_guard
from stock_research.config import Settings
from stock_research.errors import ResearchSourceChanged
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.schema import RESEARCH_RUNS
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for


def test_nested_and_same_named_sources_change_fingerprint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "schemas").mkdir()
    top = tmp_path / "api.py"
    nested = tmp_path / "schemas" / "api.py"
    top.write_text("value = 1\n")
    nested.write_text("value = 2\n")
    monkeypatch.setattr(source_guard, "__file__", str(tmp_path / "source_guard.py"))
    first = source_guard.source_identity()
    monkeypatch.setattr(source_guard, "LOADED_SOURCE_IDENTITY", first)
    source_guard.ensure_source_current()
    nested.write_text("value = 3\n")
    second = source_guard.source_identity()
    assert second != first
    with pytest.raises(ResearchSourceChanged, match="重启"):
        source_guard.ensure_source_current()
    top.write_text("value = 4\n")
    assert source_guard.source_identity() not in {first, second}


def test_martin_cache_recomputes_for_new_source_identity(
    snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, parameters = tmp_path / "snapshot.json", tmp_path / "parameters.json"
    path.write_text(snapshot.model_dump_json())
    parameters.write_text(MartinParameters().model_dump_json())
    settings = Settings(_env_file=None, artifacts_path=tmp_path / "artifacts", parameters_path=parameters)
    compute, calls = research_commands.compute_martin, []

    def observed(*args):
        calls.append(True)
        return compute(*args)

    monkeypatch.setattr(research_commands, "compute_martin", observed)
    first = research_commands.analyze_command(settings, path, snapshot.as_of)
    assert research_commands.analyze_command(settings, path, snapshot.as_of)["path"] == first["path"]
    assert len(calls) == 1
    monkeypatch.setattr(research_commands, "LOADED_SOURCE_IDENTITY", "synthetic-new-source")
    second = research_commands.analyze_command(settings, path, snapshot.as_of)
    assert second["path"] != first["path"] and len(calls) == 2
    assert Path(first["path"]).exists() and Path(second["path"]).exists()


@pytest.mark.usefixtures("local_postgres_socket")
def test_stale_api_rejects_actions_but_retains_report_reads(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token"}),
                "workspace": "guard-" + new_id(),
                "postgres_database": "stock_research_test",
                "artifacts_path": tmp_path,
            },
            api_token="synthetic-guard-operator-with-32-characters",
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            run = await run_preview(engine, settings, snapshot, analyses_for(snapshot), snapshot.as_of.replace(day=23))
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer synthetic-guard-operator-with-32-characters"},
                ) as client,
            ):
                monkeypatch.setattr(source_guard, "source_identity", lambda: "synthetic-stale-code")
                assert (await client.get("/healthz")).status_code == 200
                assert (await client.get("/readyz")).status_code == 503
                assert (await client.get("/v1/reports/latest")).status_code == 200
                rejected = await client.post(
                    "/v1/research-runs",
                    headers={"Idempotency-Key": "synthetic-source-guard-id"},
                    json={"market": "us", "target_session": "2026-11-23", "preview": True},
                )
                assert rejected.status_code == 503 and "重启" in rejected.json()["message"]
                assert (await client.post(f"/v1/research-runs/{run['run_id']}/retry")).status_code == 503
                async with engine.connect() as connection:
                    assert (
                        await connection.scalar(
                            select(func.count())
                            .select_from(RESEARCH_RUNS)
                            .where(RESEARCH_RUNS.c.workspace == settings.workspace)
                        )
                        == 1
                    )
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
