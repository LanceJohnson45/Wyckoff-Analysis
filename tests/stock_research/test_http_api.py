"""实际ASGI生命周期和真实临时PostgreSQL的认证、隔离及固定修订查询。"""

import asyncio
from pathlib import Path

import httpx
import pytest

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.pipeline import run_preview
from stock_research.queries import latest_report_id
from stock_research.run_store import RunStore
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
TOKEN = "synthetic-api-token-with-32-characters"


def test_api_requires_independent_nonempty_bearer_configuration() -> None:
    with pytest.raises(ValueError, match="32字符"):
        create_app(Settings(_env_file=None, api_token="short"))


def test_authenticated_http_reads_saved_reports_and_fixed_revisions(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    async def scenario() -> None:
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token"}),
                "workspace": "http-" + new_id(),
                "artifacts_path": tmp_path,
                "postgres_database": "stock_research_test",
            },
            api_token=TOKEN,
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            result = await run_preview(
                engine, settings, snapshot, analyses_for(snapshot), snapshot.as_of.replace(day=23)
            )
            store = RunStore(engine, settings.workspace)
            first_report = await latest_report_id(store)
            await store.publish(result["run_id"], "partial")
            latest = await latest_report_id(store)
            assert latest != first_report
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
            ):
                assert (await client.get("/healthz")).status_code == 200
                assert (await client.get("/readyz")).json()["data"]["status"] == "ready"
                assert (await client.get("/v1/reports/latest")).status_code == 401
                headers = {"Authorization": f"Bearer {TOKEN}"}
                assert (
                    await client.get("/v1/reports/latest", headers={**headers, "Origin": "https://untrusted.invalid"})
                ).status_code == 403
                response = await client.get("/v1/reports/latest", headers=headers)
                assert response.status_code == 200
                body = response.json()
                assert set(body) == {"code", "message", "data", "correlation_id"}
                assert body["code"] == "0000"
                assert body["data"]["report_id"] == latest
                assert body["data"]["run"]["status"] == "partial"
                assert body["data"]["run"]["quality"]["passed"] is True
                fixed = (await client.get(f"/v1/reports/{first_report}", headers=headers)).json()["data"]
                assert fixed["run"]["status"] == "completed"
                assert len(fixed["run"]["selections"]) == len(fixed["run"]["reviews"]) == 2
                assert (
                    TOKEN not in response.text
                    and "receipt_path" not in response.text
                    and "base_url" not in response.text
                )
                runs = (await client.get("/v1/research-runs?page=1&page_size=1", headers=headers)).json()["data"]
                assert runs["total"] == 1 and runs["items"][0]["run_id"] == result["run_id"]
                for path in (
                    "/v1/research-runs?page=0",
                    "/v1/research-runs/not-a-uuid",
                    "/v1/research-runs?page_size=101",
                ):
                    invalid = await client.get(path, headers=headers)
                    assert invalid.status_code == 422
                    assert invalid.json()["code"] == "research.invalid_input"
                missing = await client.get(f"/v1/research-runs/{new_id()}", headers=headers)
                assert missing.status_code == 404
                assert missing.json()["data"] is None
                spoofed = await client.get("/healthz", headers={"Host": "untrusted.invalid"})
                assert spoofed.status_code == 403
                events = await client.get(
                    f"/v1/research-runs/{result['run_id']}/events",
                    headers={**headers, "Last-Event-ID": str(body["data"]["revision"] - 1)},
                )
                assert events.headers["content-type"].startswith("text/event-stream")
                assert events.text.count("event: report") == 1
                assert f"id: {body['data']['revision']}" in events.text
                empty = await client.get(
                    f"/v1/research-runs/{result['run_id']}/events",
                    headers={**headers, "Last-Event-ID": str(body["data"]["revision"])},
                )
                assert "event: report" not in empty.text
            foreign = create_app(settings.model_copy(update={"workspace": "foreign"}))
            async with (
                foreign.router.lifespan_context(foreign),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=foreign), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                assert (await client.get(f"/v1/reports/{latest}")).status_code == 404
                assert (await client.get(f"/v1/research-runs/{result['run_id']}")).status_code == 404
                assert (await client.get("/v1/research-runs")).json()["data"]["total"] == 0
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
