"""知识检索授权接线；HTTP只经MockTransport，不发起真实网络。"""

import asyncio
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from stock_research.chat_evidence import ChatAudit, ChatEvidence
from stock_research.chat_mcp import CHAT_TOOLS, chat_tools, create_server
from stock_research.chat_runtime import prepare_chat_files
from stock_research.config import Settings
from stock_research.identity import digest
from stock_research.knowledge_client import KnowledgeClient
from stock_research.knowledge_settings import KnowledgeSettings
from stock_research.model_connection import ModelConnection
from stock_research.schemas.chat import ChatContext

READER = "knowledge-reader-token-with-at-least-32-characters"
ANSWER = {
    "results": [
        {
            "document_id": "doc-1",
            "source_id": "source-1",
            "revision": 2,
            "title": "Wyckoff phase note",
            "evidence": "volume dried before the spring",
            "valid_from": "2026-11-25T00:00:00+00:00",
            "chunk_id": "ignored-index-metadata",
        }
    ],
    "backend": "pg-mirror-hybrid",
    "generation": 3,
}


def knowledge_settings() -> KnowledgeSettings:
    return KnowledgeSettings(base_url="http://127.0.0.1:9000/", token=READER)


def chat_context() -> ChatContext:
    body = {
        "workspace": "stocks",
        "run_id": uuid4(),
        "report_id": uuid4(),
        "stage_id": uuid4(),
        "snapshot_id": uuid4(),
        "snapshot_hash": "a" * 64,
        "as_of": date(2026, 11, 24),
        "target_session": date(2026, 11, 24),
        "question": "固定报告问题",
        "history": (),
        "deadline": datetime(2026, 11, 25, tzinfo=UTC),
        "tool_budget": 20,
        "api_port": 8788,
    }
    probe = ChatContext.model_construct(**body, content_hash="0" * 64)
    return ChatContext(**body, content_hash=digest(probe.model_dump(mode="json", exclude={"content_hash"})))


def test_unconfigured_tools_stay_exact() -> None:
    assert chat_tools(None) == CHAT_TOOLS
    assert Settings(_env_file=None).knowledge_settings is None
    assert chat_tools(knowledge_settings()) == (*CHAT_TOOLS, "mcp__chat__knowledge_search")


def test_rejects_partial_or_remote_authorization() -> None:
    settings = Settings(_env_file=None, knowledge_base_url="http://127.0.0.1:9000")
    with pytest.raises(ValueError, match="知识检索入口"):
        _ = settings.knowledge_settings
    remote = knowledge_settings().model_copy(update={"base_url": "https://knowledge.example/knowledge"})
    with pytest.raises(ValueError, match="知识服务入口"):
        _ = remote.endpoint
    with pytest.raises(ValueError, match="不能复用研究API身份"):
        _ = Settings(
            _env_file=None,
            knowledge_base_url="http://127.0.0.1:9000",
            knowledge_read_token=READER,
            api_read_token=READER,
        ).knowledge_settings


def test_knowledge_identity_is_file_scoped_and_not_in_profile(tmp_path: Path) -> None:
    settings = Settings(
        _env_file=None,
        artifacts_path=tmp_path,
        dsh_home=tmp_path / "dsh",
        knowledge_base_url="http://127.0.0.1:9000",
        knowledge_read_token=READER,
    )
    connection = ModelConnection(provider="fixture", model="fixture", base_url="http://unused", api_key="fixture-key")
    files = prepare_chat_files(
        settings,
        chat_context(),
        connection,
        "research-query-token-with-32-characters",
        knowledge=settings.knowledge_settings,
    )
    profile = (files.home / "profiles/research/cordis.patch.yml").read_text()
    assert files.knowledge is not None and str(files.knowledge) in profile and READER not in profile
    assert files.knowledge.stat().st_mode & 0o777 == 0o600
    assert KnowledgeSettings.model_validate_json(files.knowledge.read_text()) == settings.knowledge_settings


def test_client_forwards_query_only_and_registers_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[httpx.Request] = []

    def service(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json=ANSWER)

    async def scenario() -> None:
        monkeypatch.setattr(
            "stock_research.chat_mcp.KnowledgeClient",
            lambda settings: KnowledgeClient(settings, transport=httpx.MockTransport(service)),
        )
        audit = ChatAudit(chat_context().content_hash, tmp_path / "evidence.json")
        server = create_server(None, chat_context(), audit, knowledge_settings())
        tools = await server.list_tools()
        assert {tool.name for tool in tools} == {item.rsplit("__", 1)[-1] for item in chat_tools(knowledge_settings())}
        assert all({"workspace", "subject", "url"}.isdisjoint(tool.inputSchema.get("properties", {})) for tool in tools)
        await server.call_tool("knowledge_search", {"query": "spring volume"})
        evidence = ChatEvidence.model_validate_json(audit.path.read_text())
        assert evidence.successes == 1
        assert any(isinstance(fact.value, str) and "volume dried" in fact.value for fact in evidence.facts.values())
        assert not any("ignored-index-metadata" in fact.model_dump_json() for fact in evidence.facts.values())

    asyncio.run(scenario())
    request = calls[0]
    assert request.method == "GET" and request.url.path == "/internal/knowledge/search"
    assert request.url.params["q"] == "spring volume"
    assert set(request.url.params) == {"q"}
    assert request.headers["Authorization"] == "Bearer " + READER


def test_client_wraps_transport_and_contract_errors() -> None:
    def unavailable(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("fixture-unavailable")

    def invalid(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [], "backend": ""})

    async def scenario(handler: Callable[[httpx.Request], httpx.Response]) -> None:
        with pytest.raises(ValueError, match="知识"):
            await KnowledgeClient(knowledge_settings(), transport=httpx.MockTransport(handler)).search("spring")

    asyncio.run(scenario(unavailable))
    asyncio.run(scenario(invalid))
