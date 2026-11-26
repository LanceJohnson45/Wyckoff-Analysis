"""MCP页必须完整保留原JSON；缓存续读不能刷新或跨查询借用。"""

import asyncio
import json
from copy import deepcopy

import pytest
from mcp.server.fastmcp import FastMCP
from pydantic import JsonValue, RootModel

from stock_research.identity import digest
from stock_research.query_client import QueryError
from stock_research.query_pages import MAX_PAGE_BYTES, QueryPages
from stock_research.schemas.query_pages import QueryPage


class Document(RootModel[dict[str, JsonValue]]):
    pass


def reconstruct(pages: list) -> dict:
    result = {}
    for page in pages:
        for chunk in page.chunks:
            node = result
            for index, key in enumerate(chunk.path[:-1]):
                if isinstance(node, dict):
                    node = node.setdefault(key, [] if isinstance(chunk.path[index + 1], int) else {})
                else:
                    while len(node) <= key:
                        node.append([] if isinstance(chunk.path[index + 1], int) else {})
                    node = node[key]
            key = chunk.path[-1]
            if isinstance(node, list):
                while len(node) <= key:
                    node.append(None)
            if chunk.text_offset is None:
                node[key] = chunk.value
            else:
                previous = node[key] if chunk.text_offset else ""
                assert len(previous) == chunk.text_offset
                node[key] = previous + chunk.value
                assert len(node[key]) <= chunk.text_total
    return result


def test_actual_sdk_pages_reconstruct_unicode_large_history_and_original_values() -> None:
    async def scenario() -> None:
        body = {"empty": {}, "list": [], "false": False, "null": None, "zero": 0, "number": 1.2345}
        body["history"] = [{"date": str(i), "ref": 'a/"/~\\行情', "close": 10 + i / 17} for i in range(320)]
        body["limitations"] = ['风险"\\\n😀' * 10000, "not PIT"]
        expected = deepcopy(body)
        document = Document(body)
        cache = QueryPages()
        server = FastMCP("page-test")

        @server.tool()
        async def read(cursor: str | None = None) -> QueryPage:
            return await cache.respond("stock", {"limit": 320}, operation, cursor)

        async def operation():
            return document

        pages, cursor = [], None
        while True:
            content, structured = await server.call_tool("read", {"cursor": cursor})
            assert len(content) == 1 and len(content[0].text.encode()) <= MAX_PAGE_BYTES
            assert json.loads(content[0].text) == structured
            page = QueryPage.model_validate(structured)
            pages.append(page)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        assert len(pages) > 2
        assert reconstruct(pages) == expected == body
        assert all(page.content_hash == digest(expected) for page in pages)
        assert [page.page for page in pages] == list(range(1, len(pages) + 1))
        assert all(page.total_pages == len(pages) for page in pages)

    asyncio.run(scenario())


def test_cursor_is_bound_to_original_read_and_arguments_and_does_not_refresh(monkeypatch) -> None:
    async def scenario() -> None:
        clock = [0.0]
        monkeypatch.setattr("stock_research.query_pages.time.monotonic", lambda: clock[0])
        body = Document({"report": "original", "history": list(range(20000))})
        cache = QueryPages()

        async def operation():
            return body

        first = await cache.respond("report", {"target": "2026-11-24"}, operation)
        assert first.next_cursor
        saved_hash = first.content_hash
        expected = deepcopy(body.root)
        body.root["report"] = "new revision"

        async def unavailable():
            raise QueryError("source unavailable after first read")

        second = await cache.respond("report", {"target": "2026-11-24"}, unavailable, first.next_cursor)
        pages = [first, second]
        cursor = second.next_cursor
        while cursor:
            page = await cache.respond("report", {"target": "2026-11-24"}, unavailable, cursor)
            pages.append(page)
            cursor = page.next_cursor
        assert reconstruct(pages) == expected
        assert second.content_hash == saved_hash
        assert second == await cache.respond("report", {"target": "2026-11-24"}, operation, first.next_cursor)
        for tool, args, cursor in (
            ("stock", {"target": "2026-11-24"}, first.next_cursor),
            ("report", {"target": "2026-11-25"}, first.next_cursor),
            ("report", {"target": "2026-11-24"}, "not-a-cursor"),
        ):
            with pytest.raises(QueryError, match="续页"):
                await cache.respond(tool, args, operation, cursor)
        with pytest.raises(QueryError, match="续页"):
            await QueryPages().respond("report", {"target": "2026-11-24"}, operation, first.next_cursor)
        clock[0] = 301
        with pytest.raises(QueryError, match="续页"):
            await cache.respond("report", {"target": "2026-11-24"}, operation, first.next_cursor)

    asyncio.run(scenario())


def test_three_original_mcp_tools_advertise_the_complete_page_contract() -> None:
    from stock_research.query_client import ResearchReader
    from stock_research.query_mcp import create_server
    from stock_research.query_settings import QuerySettings

    async def scenario() -> None:
        server = create_server(
            ResearchReader(QuerySettings(_env_file=None, token="synthetic-query-token-with-32-characters"))
        )
        tools = await server.list_tools()
        assert len(tools) == 3
        for tool in tools:
            assert "cursor" in tool.inputSchema["properties"]
            assert {"schema_version", "content_hash", "scope", "chunks", "next_cursor"} <= tool.outputSchema[
                "properties"
            ].keys()
            assert tool.annotations.readOnlyHint and not tool.annotations.openWorldHint
        assert not await server.list_resources()

    asyncio.run(scenario())


@pytest.mark.parametrize("capacity", ["entries", "bytes"])
def test_evicted_results_fail_closed_and_retained_cursor_still_reads(capacity, monkeypatch) -> None:
    async def scenario() -> None:
        from stock_research.query_pages import build_pages

        body = Document({"history": list(range(2000))})
        size = sum(len(p.model_dump_json(indent=2).encode()) for p in build_pages("stock", body))
        monkeypatch.setattr("stock_research.query_pages.MAX_CACHED_RESULTS", 2 if capacity == "entries" else 8)
        monkeypatch.setattr(
            "stock_research.query_pages.MAX_CACHED_BYTES", size * 2 + 100 if capacity == "bytes" else size * 10
        )
        cache = QueryPages()

        async def read():
            return body

        first = await cache.respond("stock", {"symbol": "AAPL"}, read)
        assert first.next_cursor
        await cache.respond("stock", {"symbol": "MSFT"}, read)
        last = await cache.respond("stock", {"symbol": "IEX"}, read)
        with pytest.raises(QueryError, match="续页"):
            await cache.respond("stock", {"symbol": "AAPL"}, read, first.next_cursor)
        assert (await cache.respond("stock", {"symbol": "IEX"}, read, last.next_cursor)).result_id == last.result_id

    asyncio.run(scenario())


def test_invalid_cursor_and_oversize_fields_are_explicit_errors(monkeypatch) -> None:
    async def scenario() -> None:
        cache = QueryPages()

        async def read():
            return Document({"value": "风险" * 3000})

        first = await cache.respond("stock", {}, read)
        for number in ("0", "-1", "01", "2:3", "9" * 100, "２", "999999"):
            with pytest.raises(QueryError, match="续页"):
                await cache.respond("stock", {}, read, f"{first.result_id}:{number}")
        monkeypatch.setattr("stock_research.query_pages.MAX_CACHED_BYTES", 1)
        with pytest.raises(QueryError, match="暂存上限"):
            await cache.respond("stock", {}, read)
        monkeypatch.setattr("stock_research.query_pages.MAX_RESPONSE_BYTES", 8)
        with pytest.raises(QueryError, match="完整分页上限"):
            await cache.respond("stock", {}, read)

    asyncio.run(scenario())
