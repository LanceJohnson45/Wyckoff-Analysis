"""原生PG→ASGI→最小只读客户端，另测实际MCP握手与工具范围。"""

import asyncio
import os
import sys
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from pydantic import ValidationError
from sqlalchemy import update

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import digest, new_id
from stock_research.models import Snapshot
from stock_research.pipeline import run_preview
from stock_research.query_client import QueryError, ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for

OPERATOR = "synthetic-operator-token-with-32-characters"
READER = "synthetic-read-only-token-with-32-characters"


@pytest.mark.usefixtures("local_postgres_socket")
def test_frozen_stock_read_client_and_readonly_identity(
    postgres_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={
                "workspace": "queries-" + new_id(),
                "artifacts_path": tmp_path,
                "postgres_database": "stock_research_test",
            }
        )
        settings = Settings(
            _env_file=None,
            **settings.model_dump(exclude={"api_token", "api_read_token"}),
            api_token=OPERATOR,
            api_read_token=READER,
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            analyses = analyses_for(snapshot)
            run = await run_preview(engine, settings, snapshot, analyses, snapshot.as_of.replace(day=23))
            app = create_app(settings)
            transport = httpx.ASGITransport(app=app)
            reader = ResearchReader(QuerySettings(_env_file=None, token=READER), transport=transport)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:8788") as client,
            ):
                report = await reader.latest_report()
                run_id = report.run.run_id
                assert str(run_id) == run["run_id"]
                assert (await reader.run_status(run_id)).status == "completed"
                stock = await reader.stock("AAPL", run_id, 5)
                assert stock.report_id is None
                assert stock.company.name == "Apple"
                assert stock.snapshot_hash == snapshot.content_hash
                assert len(stock.history) == 5 < stock.history_total
                assert all(bar.session_date <= stock.as_of for bar in stock.history)
                observed = stock.observations[0]
                expected = next(row for row in analyses["martin"].observations if row.symbol == "AAPL")
                price = next(row for row in observed.facts if row.ref == "martin/AAPL/factors/close")
                assert price.value == expected.factors["close"]
                assert observed.calculation_available is True
                assert not stock.selections and not stock.reviews
                headers = {"Authorization": f"Bearer {READER}"}
                for path in (
                    "/v1/research-runs",
                    f"/v1/research-runs/{run_id}/retry",
                    f"/v1/research-runs/{run_id}/cancel",
                ):
                    assert (await client.post(path, headers=headers, json={})).status_code == 403
                for method in ("PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"):
                    assert (await client.request(method, "/v1/research-runs", headers=headers)).status_code == 403
                base = f"/v1/stocks/AAPL/observations?run_id={run_id}"
                fixed = await client.get(base + f"&report_id={report.report_id}", headers=headers)
                assert fixed.json()["data"]["report_id"] == str(report.report_id)
                mismatched = base.replace(str(run_id), new_id()) + f"&report_id={report.report_id}"
                assert (await client.get(mismatched, headers=headers)).status_code == 404
                assert (await client.get(base + "&limit=321", headers=headers)).status_code == 422
                assert (await client.get(base.replace("AAPL", "ZZZZ"), headers=headers)).status_code == 404
                assert (await client.get(base.replace("AAPL", "aapl"), headers=headers)).status_code == 422
                with pytest.raises(QueryError, match="无效"):
                    await reader.stock("../.env", run_id)
                with pytest.raises(QueryError, match="未成功"):
                    await reader.stock("ZZZZ", run_id)
                store = RunStore(engine, settings.workspace)
                saved = await store.row(RESEARCH_RUNS, str(run_id))
                changed = {**saved["payload"], "analysis_hashes": {"martin": "0" * 64, "wyckoff": "0" * 64}}
                async with engine.begin() as connection:
                    await connection.execute(
                        update(RESEARCH_RUNS)
                        .where(RESEARCH_RUNS.c.uuid == str(run_id))
                        .values(payload=changed, content_hash=digest(changed))
                    )
                assert (await client.get(base, headers=headers)).status_code == 503
            foreign = create_app(settings.model_copy(update={"workspace": "foreign"}))
            async with (
                foreign.router.lifespan_context(foreign),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=foreign), base_url="http://127.0.0.1:8788"
                ) as client,
            ):
                assert (await client.get(base, headers=headers)).status_code == 404
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_readonly_configuration_cannot_reuse_operator_or_arbitrary_host() -> None:
    with pytest.raises(ValueError, match="独立"):
        create_app(Settings(_env_file=None, api_token=OPERATOR, api_read_token=OPERATOR))
    with pytest.raises(ValidationError):
        QuerySettings(_env_file=None, host="untrusted.invalid", token=READER)
    with pytest.raises(ValidationError):
        QuerySettings(_env_file=None, token="short")


def test_actual_query_mcp_exports_only_three_read_tools(tmp_path: Path) -> None:
    async def scenario() -> None:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "stock_research.query_mcp"],
            env={"PATH": os.environ.get("PATH", ""), "STOCK_RESEARCH_QUERY_TOKEN": READER},
            cwd=tmp_path,
        )
        async with stdio_client(parameters) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            tools = (await session.list_tools()).tools
            assert {tool.name for tool in tools} == {
                "stock_research_latest_report",
                "stock_research_stock",
                "stock_research_run_status",
            }
            assert all(tool.annotations.readOnlyHint and not tool.annotations.openWorldHint for tool in tools)
            denied = await session.call_tool("stock_research_stock", {"symbol": "../.env", "run_id": new_id()})
            assert denied.isError
            assert not (await session.list_resources()).resources

    asyncio.run(scenario())
