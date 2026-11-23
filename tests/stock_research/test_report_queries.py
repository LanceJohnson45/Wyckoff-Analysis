"""原生PG验证固定报告市场、分组、比较和规则，不调用行情网络或模型。"""

import asyncio
import os
import sys
from pathlib import Path

import httpx
import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from sqlalchemy import update

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameter_store import activate_parameters, register_parameters
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.quality import build_snapshot
from stock_research.query_client import ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.run_store import RunStore
from stock_research.schema import FACTOR_RESULTS
from stock_research.schemas.parameters import CreateParameterSet
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_queries import OPERATOR, READER
from tests.stock_research.test_run_store import analyses_for


@pytest.fixture
def grouped_snapshot(snapshot: Snapshot) -> Snapshot:
    """四只合成股票产生两行业和一含特殊字符的已确认主题。"""
    symbols = ("AAPL", "MSFT", "NVDA", "TINY")
    universe = snapshot.universe.model_copy(
        update={
            "members": tuple(
                member.model_copy(update={"symbol": symbol, "name": "Fixture " + symbol})
                for symbol in symbols
                for member in snapshot.universe.members
            )
        }
    )
    classifications = snapshot.classifications.model_copy(
        update={
            "records": tuple(
                snapshot.classifications.records[0].model_copy(
                    update={
                        "symbol": symbol,
                        "company_name": "Fixture " + symbol,
                        "industry": "Hardware" if symbol != "TINY" else "Tiny",
                        "themes": ("Devices');半导体",),
                    }
                )
                for symbol in symbols
            )
        }
    )
    bars = tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + tuple(
        bar.model_copy(update={"symbol": symbol}) for symbol in symbols for bar in snapshot.bars if bar.kind == "stock"
    )
    return build_snapshot(universe, classifications, bars, "synthetic-group-queries", snapshot.as_of)


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_fixed_report_queries_match_original_artifacts(
    postgres_settings: Settings, grouped_snapshot: Snapshot, tmp_path: Path
) -> None:
    """【正常值】实际客户端读取原数值和SQL分页，不因当前发布或新修订改写历史。"""

    async def scenario() -> None:
        settings = Settings(
            _env_file=None,
            **{
                **postgres_settings.model_dump(exclude={"api_token", "api_read_token"}),
                "workspace": "report-queries-" + new_id(),
                "postgres_database": "stock_research_test",
                "artifacts_path": tmp_path,
            },
            api_token=OPERATOR,
            api_read_token=READER,
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            analyses = analyses_for(grouped_snapshot)
            await run_preview(engine, settings, grouped_snapshot, analyses, grouped_snapshot.as_of.replace(day=23))
            app = create_app(settings)
            transport = httpx.ASGITransport(app=app)
            reader = ResearchReader(QuerySettings(_env_file=None, token=READER), transport=transport)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=transport, base_url="http://127.0.0.1:8788", headers={"Authorization": "Bearer " + READER}
                ) as client,
            ):
                report = await reader.latest_report()
                market = await reader.market_overview(report.report_id, 5)
                assert market.snapshot_hash == grouped_snapshot.content_hash
                assert [row.symbol for row in market.benchmarks] == ["SPY", "QQQ", "IWM"]
                assert all(len(row.history) == 5 and row.history_total == 320 for row in market.benchmarks)
                assert market.benchmarks[0].history[-1].close == next(
                    bar.close
                    for bar in grouped_snapshot.bars
                    if bar.kind == "benchmark" and bar.symbol == "SPY" and bar.session_date == grouped_snapshot.as_of
                )
                assert {item.ref: item.value for item in market.market_facts}["market/SPY/regime"] == analyses[
                    "wyckoff"
                ].metrics["benchmark"]["regime"]
                assert [item.symbol for item in market.strategies[0].pool] == list(analyses["martin"].pool)
                groups = await reader.group_strength(report.report_id, 1, 2)
                tail = await reader.group_strength(report.report_id, 2, 2)
                assert groups.total == tail.total == 3 and len(groups.items) == 2 and len(tail.items) == 1
                assert (
                    tail.items[0].name == "industry:Tiny" and tail.items[0].score is None and not tail.items[0].usable
                )
                original = {row["name"]: row for row in analyses["martin"].groups}
                assert all(
                    item.model_dump(exclude={"ref_prefix"}, mode="json") == original[item.name]
                    for item in (*groups.items, *tail.items)
                )
                named = await reader.group_strength(report.report_id, kind="theme", name="theme:Devices');半导体")
                assert named.total == 1 and len(named.items[0].valid_members) == 4
                assert (await reader.group_strength(report.report_id, name="x' OR 1=1 --")).total == 0
                assert not (await reader.group_strength(report.report_id, page=10)).items
                comparison = await reader.compare_stocks(report.report_id, ("MSFT", "AAPL"), 3)
                assert [item.company.symbol for item in comparison.items] == ["MSFT", "AAPL"]
                assert all(
                    item.snapshot_hash == market.snapshot_hash
                    and item.report_id == report.report_id
                    and len(item.history) == 3
                    for item in comparison.items
                )
                assert comparison.items[1] == await reader.stock("AAPL", report.run.run_id, 3, report.report_id)
                rules = await reader.strategy_rules(report.report_id)
                assert rules.strategies[0].parameters == analyses["martin"].parameters
                assert rules.strategies[0].factor_definitions == analyses["martin"].factor_definitions
                assert rules.strategies[1].effective_config == analyses["wyckoff"].effective_config
                store = RunStore(engine, settings.workspace)
                registered = await register_parameters(
                    store,
                    CreateParameterSet(
                        strategy="martin", parameters=MartinParameters(version="changed", support_atr=0.5)
                    ),
                )
                await activate_parameters(store, "martin", str(registered.id), 0)
                await store.publish(str(report.run.run_id), "partial")
                assert await reader.strategy_rules(report.report_id) == rules
                assert await reader.report(report.report_id) == report
                assert (
                    await client.get(f"/v1/reports/{report.report_id}/compare", params={"symbols": ["AAPL", "ZZZZ"]})
                ).status_code == 404
                for query in (
                    {"symbols": ["AAPL", "AAPL"]},
                    {"symbols": ["AAPL"]},
                    {"symbols": ["AAPL", "../.env"]},
                    {"symbols": ["AAPL", "MSFT"], "limit": 61},
                ):
                    assert (
                        await client.get(f"/v1/reports/{report.report_id}/compare", params=query)
                    ).status_code == 422
                for resource in ("market", "groups", "rules"):
                    assert (
                        await client.get(f"/v1/reports/{report.report_id}/{resource}", headers={"Authorization": ""})
                    ).status_code == 401
                    assert (await client.post(f"/v1/reports/{report.report_id}/{resource}")).status_code == 403
                assert (await client.get(f"/v1/reports/{report.report_id}/groups?page_size=101")).status_code == 422
                assert (await client.get(f"/v1/reports/{report.report_id}/groups?kind=other")).status_code == 422
                foreign = create_app(settings.model_copy(update={"workspace": "foreign-" + new_id()}))
                async with (
                    foreign.router.lifespan_context(foreign),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=foreign),
                        base_url="http://127.0.0.1:8788",
                        headers={"Authorization": "Bearer " + READER},
                    ) as foreign_client,
                ):
                    for resource in ("market", "groups", "rules", "compare?symbols=AAPL&symbols=MSFT"):
                        assert (
                            await foreign_client.get(f"/v1/reports/{report.report_id}/{resource}")
                        ).status_code == 404
                async with engine.begin() as connection:
                    await connection.execute(
                        update(FACTOR_RESULTS)
                        .where(FACTOR_RESULTS.c.uuid == str(rules.strategies[0].artifact_id))
                        .values(content_hash="f" * 64)
                    )
                for resource in ("market", "groups", "rules", "compare?symbols=AAPL&symbols=MSFT"):
                    assert (await client.get(f"/v1/reports/{report.report_id}/{resource}")).status_code == 503
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_research_mcp_has_eight_readonly_tools(tmp_path: Path) -> None:
    """【正常值】研究专用入口真实握手，原Personal三项入口另有保留范围回归。"""

    async def scenario() -> None:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "stock_research.research_mcp"],
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
                "stock_research_report_read",
                "stock_research_market_overview",
                "stock_research_group_strength",
                "stock_research_compare_stocks",
                "stock_research_strategy_rules",
            }
            assert all(tool.annotations.readOnlyHint and not tool.annotations.openWorldHint for tool in tools)
            denied = await session.call_tool(
                "stock_research_compare_stocks", {"report_id": new_id(), "symbols": ["AAPL", "../.env"]}
            )
            assert denied.isError and not (await session.list_resources()).resources

    asyncio.run(scenario())
