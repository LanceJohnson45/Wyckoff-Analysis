"""原生PG验证整池缺数保留、历史初选边界、SQL字面搜索与绑定MCP读取。"""

import asyncio
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import update

from stock_research.chat_evidence import ChatAudit, ChatEvidence
from stock_research.chat_manager import ChatManager
from stock_research.chat_mcp import create_server
from stock_research.chat_store import ChatStore
from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.pipeline import prepare_preview, resume_preview
from stock_research.quality import build_snapshot
from stock_research.query_client import ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS
from stock_research.schemas.chat import CreateChatTurn
from stock_research.schemas.universe import UniverseQuery
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_queries import OPERATOR, READER
from tests.stock_research.test_run_store import analyses_for


@pytest.fixture
def incomplete_universe(snapshot: Snapshot) -> Snapshot:
    """显式合成低覆盖夹具，不修改operator门槛；过期/未来成员不进入有效并集。"""
    members = tuple(
        member.model_copy(update={"symbol": symbol, "name": "Fixture " + symbol})
        for symbol in ("AAPL", "MSFT", "FUTR", "LOST")
        for member in snapshot.universe.members
    ) + (
        snapshot.universe.members[0].model_copy(update={"symbol": "OLD", "expiry_date": snapshot.as_of}),
        snapshot.universe.members[0].model_copy(
            update={"symbol": "NEW", "effective_date": snapshot.as_of + timedelta(days=1)}
        ),
    )
    records = (
        snapshot.classifications.records[0].model_copy(update={"company_name": "Apple 100%_'"}),
        snapshot.classifications.records[0].model_copy(
            update={
                "symbol": "FUTR",
                "company_name": "Never show future name",
                "effective_date": snapshot.as_of + timedelta(days=1),
            }
        ),
    )
    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + tuple(
        bar.model_copy(update={"symbol": symbol})
        for symbol in ("AAPL", "MSFT", "FUTR")
        for bar in snapshot.bars
        if bar.kind == "stock"
    )
    return build_snapshot(
        snapshot.universe.model_copy(update={"members": members}),
        snapshot.classifications.model_copy(update={"records": records}),
        bars,
        "synthetic-incomplete-universe",
        snapshot.as_of,
        coverage_min=0.75,
    )


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_universe_keeps_missing_members_and_fixed_selection_state(
    postgres_settings: Settings, incomplete_universe: Snapshot, tmp_path: Path
) -> None:
    """【正常/异常值】真实冻结、PG分页、权限和MCP；缺数不会被内连接或布尔默认值隐藏。"""

    async def scenario() -> None:
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token", "api_read_token"}),
                "workspace": "universe-" + new_id(),
                "postgres_database": "stock_research_test",
                "artifacts_path": tmp_path,
            },
            api_token=OPERATOR,
            api_read_token=READER,
        )
        engine = database_engine(settings)
        store = RunStore(engine, settings.workspace)
        try:
            await initialize_schema(engine)
            analyses = analyses_for(incomplete_universe)
            assert all(not item.pool for item in analyses.values())
            run_id, _connections = await prepare_preview(
                engine, settings, incomplete_universe, analyses, incomplete_universe.as_of.replace(day=23)
            )
            await store.publish(run_id, "pending")
            app = create_app(settings)
            reader = ResearchReader(QuerySettings(_env_file=None, token=READER), transport=httpx.ASGITransport(app=app))
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + READER},
                ) as client,
            ):
                frozen = await reader.latest_report()
                first = await reader.universe(frozen.report_id, UniverseQuery(page_size=2))
                tail = await reader.universe(frozen.report_id, UniverseQuery(page=2, page_size=2))
                assert first.total == first.universe_total == tail.total == 4
                assert [row.company.symbol for row in (*first.items, *tail.items)] == ["AAPL", "FUTR", "LOST", "MSFT"]
                rows = {row.company.symbol: row for row in (*first.items, *tail.items)}
                assert rows["LOST"].history_rows == 0 and rows["LOST"].last_session is None
                assert rows["FUTR"].industry is None and rows["FUTR"].company.name == "Fixture FUTR"
                assert all(len(row.indices) == 2 for row in rows.values())
                assert all(item.selected is None for row in rows.values() for item in row.strategies)
                for symbol, row in rows.items():
                    stock = await reader.stock(symbol, frozen.run.run_id, 1, frozen.report_id)
                    assert row.company == stock.company and row.history_rows == stock.history_total
                    assert [item.calculation_available for item in row.strategies] == [
                        item.calculation_available for item in stock.observations
                    ]
                    assert [item.in_pool for item in row.strategies] == [item.in_pool for item in stock.observations]
                for search in ("apple", "aapl", "%", "_", "'"):
                    found = await reader.universe(frozen.report_id, UniverseQuery(search=search))
                    assert found.total == 1 and found.items[0].company.symbol == "AAPL"
                assert (await reader.universe(frozen.report_id, UniverseQuery(search="%' OR 1=1 --"))).total == 0
                assert not (await reader.universe(frozen.report_id, UniverseQuery(page=3, page_size=2))).items
                for params in (
                    {"page_size": 101},
                    {"page": 0},
                    {"page": 1_000_001},
                    {"search": " "},
                    {"search": "x" * 101},
                    {"search": "\x00"},
                    {"other": "ignored"},
                ):
                    assert (
                        await client.get(f"/v1/reports/{frozen.report_id}/universe", params=params)
                    ).status_code == 422
                assert (
                    await client.get(f"/v1/reports/{frozen.report_id}/universe", headers={"Authorization": ""})
                ).status_code == 401
                assert (await client.post(f"/v1/reports/{frozen.report_id}/universe")).status_code == 403
                chats = ChatStore(store)
                turn = await chats.create(
                    "fixture-universe-tool", CreateChatTurn(report_id=frozen.report_id, question="查看全部冻结成员")
                )
                context = await ChatManager(store, settings).context(turn)
                audit = ChatAudit(context.content_hash, tmp_path / "universe-tool-evidence.json")
                tool = create_server(reader, context, audit)
                result = await tool.call_tool("market_overview", {"view": "universe", "search": "LOST"})
                assert result
                evidence = ChatEvidence.model_validate_json(audit.path.read_text())
                assert evidence.successes == 1 and evidence.facts["universe/LOST/history_rows"].value == 0
                await resume_preview(engine, settings, run_id)
                latest = await reader.latest_report()
                assert latest.report_id != frozen.report_id
                assert all(
                    item.selected is False
                    for row in (await reader.universe(latest.report_id)).items
                    for item in row.strategies
                )
                assert await reader.universe(frozen.report_id, UniverseQuery(page_size=2)) == first
                foreign = create_app(settings.model_copy(update={"workspace": "foreign-" + new_id()}))
                async with (
                    foreign.router.lifespan_context(foreign),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=foreign),
                        base_url="http://127.0.0.1:8788",
                        headers={"Authorization": "Bearer " + READER},
                    ) as outsider,
                ):
                    assert (await outsider.get(f"/v1/reports/{frozen.report_id}/universe")).status_code == 404
                artifact_id = (await store.summary(run_id))["manifest"]["factor_results"]["martin"]
                async with engine.begin() as connection:
                    await connection.execute(
                        update(FACTOR_RESULTS).where(FACTOR_RESULTS.c.uuid == artifact_id).values(content_hash="f" * 64)
                    )
                assert (await client.get(f"/v1/reports/{frozen.report_id}/universe")).status_code == 503
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
