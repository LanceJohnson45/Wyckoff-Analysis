"""研究问答的真实PG/ASGI权限与状态；外部模型不进入pytest网络。"""

import asyncio
import json
from pathlib import Path

import httpx
import pytest
from pydantic import BaseModel

from stock_research.chat_evidence import ChatAudit, ChatEvidence, validate_chat_answer
from stock_research.chat_manager import ChatManager
from stock_research.chat_mcp import CHAT_TOOLS, create_server
from stock_research.chat_runtime import prepare_chat_files
from stock_research.chat_store import ChatStore
from stock_research.config import Settings
from stock_research.decisions import ResearchPoint
from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.model_connection import ModelConnection
from stock_research.models import Snapshot
from stock_research.pipeline import run_preview
from stock_research.queries import latest_report_id
from stock_research.query_client import ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.run_store import RunStore
from stock_research.schema import CHAT_TURNS, RESEARCH_REPORTS
from stock_research.schemas.chat import ChatAnswer, ChatContext, ChatResult, CreateChatTurn
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_queries import OPERATOR, READER
from tests.stock_research.test_run_store import analyses_for


def synthetic_answer(context_hash: str) -> ChatAnswer:
    """模型传输桩只用于存储/取消验证，事实验证另用真实已读工具结果。"""
    return ChatAnswer(
        context_hash=context_hash,
        answer=ResearchPoint(text="固定合成观点", evidence_refs=("fixture/value",)),
        observations=(),
        risks=(),
        confirmation_conditions=(),
        claims=({"ref": "fixture/value", "value": 11},),
    )


def test_validation_chat_evidence_is_read_and_exact(tmp_path: Path) -> None:
    """【参数校验】实际工具返回后才注册引用，错误数值/布尔/自由数字均拒收。"""

    class Data(BaseModel):
        value: float
        flag: bool
        members: list[str]

    async def scenario() -> None:
        audit = ChatAudit("a" * 64, tmp_path / "evidence.json")

        async def query() -> Data:
            return Data(value=11, flag=False, members=["AAPL"])

        output = await audit.respond("fixture", query)
        evidence = ChatEvidence.model_validate_json(audit.path.read_text())
        value = next(item for item in output.facts if item.ref.endswith("/value"))
        flag = next(item for item in output.facts if item.ref.endswith("/flag"))
        point = {"text": "已读取固定事实", "evidence_refs": [value.ref]}
        body = {
            "context_hash": "a" * 64,
            "answer": point,
            "observations": [],
            "risks": [],
            "confirmation_conditions": [],
            "claims": [{"ref": value.ref, "value": 11}],
        }

        assert validate_chat_answer(json.dumps(body), evidence, "a" * 64).claims[0].value == 11
        assert evidence.calls == evidence.successes == 1
        assert audit.path.stat().st_mode & 0o777 == 0o600
        for invalid in (
            {**body, "claims": [{"ref": value.ref, "value": 12}]},
            {**body, "claims": [{"ref": flag.ref, "value": 0}]},
            {**body, "answer": {**point, "evidence_refs": ["../.env"]}},
            {**body, "answer": {**point, "text": "价格为11"}},
        ):
            with pytest.raises(ValueError):
                validate_chat_answer(json.dumps(invalid), evidence, "a" * 64)

    asyncio.run(scenario())


@pytest.fixture
def chat_settings(postgres_settings: Settings, tmp_path: Path, snapshot: Snapshot, market_path: Path) -> Settings:
    """状态使用原生临时PG，模型连接身份为明确合成数据。"""
    universe, classifications = tmp_path / "universe.json", tmp_path / "classifications.json"
    universe.write_text(snapshot.universe.model_dump_json())
    classifications.write_text(snapshot.classifications.model_dump_json())
    return Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(exclude={"api_token", "api_read_token"}),
            "workspace": "chat-" + new_id(),
            "postgres_database": "stock_research_test",
            "artifacts_path": tmp_path,
            "dsh_home": tmp_path / "dsh",
            "dsh_agents_home": tmp_path / "agents",
            "local_market_path": market_path,
            "universe_path": universe,
            "classification_path": classifications,
        },
        api_token=OPERATOR,
        api_read_token=READER,
    )


async def chat_report(settings: Settings, snapshot: Snapshot) -> tuple[RunStore, str]:
    """真正冻结合成行情和两策略产物，空池不会调用外部模型。"""
    store = RunStore(database_engine(settings), settings.workspace)
    await initialize_schema(store.engine)
    await run_preview(store.engine, settings, snapshot, analyses_for(snapshot), snapshot.as_of.replace(day=23))
    return store, await latest_report_id(store)


@pytest.mark.usefixtures("local_postgres_socket")
def test_idempotent_chat_parent_cas_is_native(chat_settings: Settings, snapshot: Snapshot) -> None:
    """【幂等性】重复键复用，父问答同时续接仅一个胜出，终态保持不可变。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        chats = ChatStore(store)
        try:
            request = CreateChatTurn(report_id=report_id, question="固定报告的问题")
            first = await chats.create("fixture-chat-root-key", request)
            assert (await chats.create("fixture-chat-root-key", request)).id == first.id
            with pytest.raises(ResearchConflict):
                await chats.create("fixture-chat-root-key", request.model_copy(update={"question": "换了问题"}))
            with pytest.raises(ResearchConflict):
                await chats.create(
                    "fixture-chat-before-end",
                    CreateChatTurn(report_id=report_id, question="不能提前续接", parent_turn_id=first.id),
                )
            completed = await chats.transition(str(first.id), "failed", error="synthetic_external_failure")
            request = CreateChatTurn(report_id=report_id, question="在同一报告继续", parent_turn_id=first.id)

            outcomes = await asyncio.gather(
                chats.create("fixture-chat-child-one", request),
                chats.create("fixture-chat-child-two", request),
                return_exceptions=True,
            )
            assert sum(isinstance(value, ResearchConflict) for value in outcomes) == 1
            child = next(value for value in outcomes if not isinstance(value, Exception))
            assert child.revision == 2 and child.conversation_id == first.id
            assert (await chats.history(child))[0].question == first.question
            assert await chats.read(str(first.id)) == completed
            assert (await chats.page(report_id, 1, 1)).total == 2
            assert len((await chats.page(report_id, 2, 1)).items) == 1
            with pytest.raises(ResearchConflict):
                await chats.transition(str(first.id), "running")
            with pytest.raises(ResearchNotFound):
                await ChatStore(RunStore(store.engine, "foreign")).read(str(first.id))
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_chat_tools_are_bound_to_original_report(
    chat_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【正常值】实际MCP工具函数经真实ASGI只读身份获取固定报告，没有报告/URL入参。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        try:
            chats = ChatStore(store)
            turn = await chats.create("fixture-bound-tools", CreateChatTurn(report_id=report_id, question="核对原报告"))
            context = await ChatManager(store, chat_settings).context(turn)
            assert context.history == () and context.report_id == turn.report_id
            app = create_app(chat_settings)
            reader = ResearchReader(QuerySettings(_env_file=None, token=READER), transport=httpx.ASGITransport(app=app))
            audit = ChatAudit(context.content_hash, tmp_path / "tool-evidence.json")
            server = create_server(reader, context, audit)
            async with app.router.lifespan_context(app):
                tools = await server.list_tools()
                assert {"mcp__chat__" + tool.name for tool in tools} == set(CHAT_TOOLS)
                assert all(
                    "report_id" not in tool.inputSchema.get("properties", {})
                    and "url" not in tool.inputSchema.get("properties", {})
                    for tool in tools
                )
                await server.call_tool("report_read", {})
                await server.call_tool("stock_factors", {"symbol": "AAPL"})

            evidence = ChatEvidence.model_validate_json(audit.path.read_text())
            assert evidence.successes == 2
            assert any(item.value == str(context.report_id) for item in evidence.facts.values())
            assert evidence.facts["martin/AAPL/factors/close"].value == 11
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_chat_http_is_private_and_persistent(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【正常值】模型传输受控，HTTP身份和PG状态走真实链路，重放不再执行模型。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        calls = []

        async def controlled(_settings: Settings, context: ChatContext, connection: ModelConnection) -> ChatResult:
            calls.append(context)
            return ChatResult(
                status="completed",
                answer=synthetic_answer(context.content_hash),
                error_code=None,
                tool_calls=1,
                context_hash=context.content_hash,
                receipt_path=str(chat_settings.artifacts_path / "synthetic-receipt.json"),
            )

        monkeypatch.setattr(
            "stock_research.chat_manager.primary_model",
            lambda _settings: ModelConnection(
                provider="fixture", model="fixture", base_url="https://example.invalid", api_key="synthetic-model-key"
            ),
        )
        monkeypatch.setattr("stock_research.chat_manager.execute_chat", controlled)
        try:
            app = create_app(chat_settings)
            headers = {"Authorization": "Bearer " + OPERATOR, "Idempotency-Key": "fixture-http-chat-key"}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                body = {"report_id": report_id, "question": "只读研究问题"}
                pending = await ChatStore(store).create(headers["Idempotency-Key"], CreateChatTurn(**body))
                created = await client.post("/v1/chat-turns", json=body)
                assert created.status_code == 202, created.text
                identifier = created.json()["data"]["id"]
                assert identifier == str(pending.id)
                async with asyncio.timeout(5):
                    while (result := (await client.get(f"/v1/chat-turns/{identifier}")).json()["data"])["status"] in {
                        "pending",
                        "running",
                    }:
                        await asyncio.sleep(0.01)

                assert result["status"] == "completed" and result["answer"]["context_hash"] == calls[0].content_hash
                assert "receipt_path" not in result and "api_key" not in result and result["receipt_available"] is True
                assert (await client.post("/v1/chat-turns", json=body)).json()["data"]["id"] == identifier and len(
                    calls
                ) == 1
                assert (await client.get("/v1/chat-turns", params={"report_id": report_id})).json()["data"][
                    "total"
                ] == 1
                for path in ("/v1/chat-turns?report_id=" + report_id, f"/v1/chat-turns/{identifier}"):
                    assert (await client.get(path, headers={"Authorization": "Bearer " + READER})).status_code == 403
                    assert (await client.get(path, headers={"Authorization": ""})).status_code == 401
                assert (
                    await client.post(
                        "/v1/chat-turns", json=body, headers={**headers, "Authorization": "Bearer " + READER}
                    )
                ).status_code == 403
                assert (await client.post("/v1/chat-turns", json={**body, "question": " "})).status_code == 422
                assert (await client.get(f"/v1/chat-turns/{new_id()}")).status_code == 404
            assert (await ChatStore(store).read(identifier)).status == "completed"
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_error_chat_capacity_replay_and_cancellation(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【异常值】满容量可重放原问答，取消等待拥有任务收尾，关闭保留终态。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        started, cleaned, ready = [], [], asyncio.Event()

        async def controlled(_settings: Settings, context: ChatContext, connection: ModelConnection) -> ChatResult:
            started.append(context.stage_id)
            if len(started) == 2:
                ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(context.stage_id)

        monkeypatch.setattr(
            "stock_research.chat_manager.primary_model",
            lambda _settings: ModelConnection(
                provider="fixture", model="fixture", base_url="https://example.invalid", api_key="synthetic-model-key"
            ),
        )
        monkeypatch.setattr("stock_research.chat_manager.execute_chat", controlled)
        try:
            app = create_app(chat_settings)
            headers = {"Authorization": "Bearer " + OPERATOR, "Idempotency-Key": "fixture-capacity-first"}
            body = {"report_id": report_id, "question": "等待中的问题"}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                first = (await client.post("/v1/chat-turns", json=body)).json()["data"]
                second = (
                    await client.post(
                        "/v1/chat-turns", json=body, headers={"Idempotency-Key": "fixture-capacity-second"}
                    )
                ).json()["data"]
                await asyncio.wait_for(ready.wait(), 5)

                assert (await client.post("/v1/chat-turns", json=body)).json()["data"]["id"] == first["id"]
                assert (
                    await client.post(
                        "/v1/chat-turns", json=body, headers={"Idempotency-Key": "fixture-capacity-third"}
                    )
                ).status_code == 409
                cancelled = await client.post(f"/v1/chat-turns/{first['id']}/cancel")
                assert cancelled.status_code == 200 and cancelled.json()["data"]["status"] == "cancelled"
                assert str(first["id"]) in {str(item) for item in cleaned}
            assert len(started) == len(cleaned) == 2
            assert (await ChatStore(store).read(second["id"])).status == "cancelled"
            assert (await ChatStore(store).page(report_id, 1, 10)).total == 2
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_error_chat_restart_preserves_live_owner_and_marks_interrupted(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【异常值】重启不接管活锁，也不重复执行失联模型；中断可独立读回。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        chats = ChatStore(store)
        try:
            turn = await chats.create(
                "fixture-interrupted-chat", CreateChatTurn(report_id=report_id, question="尚未确认的模型请求")
            )
            monkeypatch.setattr(
                "stock_research.chat_manager.primary_model", lambda _settings: pytest.fail("重启不应重新调用模型")
            )
            app = create_app(chat_settings)
            async with store.execution_lock(str(turn.id), table=CHAT_TURNS), app.router.lifespan_context(app):
                assert (await chats.read(str(turn.id))).status == "pending"
            restarted = create_app(chat_settings)

            async with restarted.router.lifespan_context(restarted):
                result = await restarted.state.chat.chats.read(str(turn.id))
                assert result.status == "failed" and result.error_code == "execution_interrupted"
                assert result.answer is None and not result.receipt_available
            assert (await chats.read(str(turn.id))).status == "failed"
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_error_research_and_chat_share_admission_capacity(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """【异常值】真实冻结入口与问答共用两席，满员拒收不改变已保存任务。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        run_started, chat_started, cleaned = asyncio.Event(), asyncio.Event(), []

        async def controlled_run(identifier: str) -> None:
            await store.publish(identifier, "running")
            run_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(identifier)

        async def controlled_chat(_settings: Settings, context: ChatContext, connection: ModelConnection) -> ChatResult:
            chat_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(str(context.stage_id))

        monkeypatch.setattr(
            "stock_research.chat_manager.primary_model",
            lambda _settings: ModelConnection(
                provider="fixture", model="fixture", base_url="https://example.invalid", api_key="synthetic-model-key"
            ),
        )
        monkeypatch.setattr("stock_research.chat_manager.execute_chat", controlled_chat)
        try:
            app = create_app(chat_settings)
            headers = {"Authorization": "Bearer " + OPERATOR}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                monkeypatch.setattr(app.state.manager, "run", controlled_run)
                original_report = await store.row(RESEARCH_REPORTS, report_id)
                run_body = {
                    "target_session": "2026-11-23",
                    "force": True,
                    "parent_run_id": original_report["run_uuid"],
                }
                accepted = await client.post(
                    "/v1/research-runs", json=run_body, headers={"Idempotency-Key": "fixture-shared-run"}
                )
                assert accepted.status_code == 202, accepted.text
                run_id = accepted.json()["data"]["run_id"]
                await asyncio.wait_for(run_started.wait(), 5)
                body = {"report_id": report_id, "question": "与研究任务并行的问题"}
                question = await client.post(
                    "/v1/chat-turns", json=body, headers={"Idempotency-Key": "fixture-shared-chat"}
                )
                assert question.status_code == 202, question.text
                await asyncio.wait_for(chat_started.wait(), 5)
                before = await store.summary(run_id)
                assert (
                    await client.post("/v1/chat-turns", json=body, headers={"Idempotency-Key": "fixture-full-chat"})
                ).status_code == 409
                assert (
                    await client.post(
                        "/v1/research-runs", json=run_body, headers={"Idempotency-Key": "fixture-full-run"}
                    )
                ).status_code == 409
                assert (await client.post(f"/v1/research-runs/{run_id}/retry")).status_code == 409
                assert await store.summary(run_id) == before
                assert (
                    await client.post(
                        "/v1/research-runs", json=run_body, headers={"Idempotency-Key": "fixture-shared-run"}
                    )
                ).json()["data"]["run_id"] == run_id
                assert (await ChatStore(store).page(report_id, 1, 10)).total == 1
                assert (await client.post(f"/v1/research-runs/{run_id}/cancel")).status_code == 202
                assert len(cleaned) == 1
            assert len(cleaned) == 2
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_normal_chat_identity_is_private_and_not_in_harness_config(chat_settings: Settings, snapshot: Snapshot) -> None:
    """【正常值】最小身份文件0600且配置只引用路径，不向Harness环境隐式转发凭据。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        try:
            turn = await ChatStore(store).create(
                "fixture-query-identity", CreateChatTurn(report_id=report_id, question="固定证据读取")
            )
            context = await ChatManager(store, chat_settings).context(turn)
            connection = ModelConnection(
                provider="fixture", model="fixture", base_url="https://example.invalid", api_key="synthetic-model-key"
            )
            files = prepare_chat_files(chat_settings, context, connection, READER)
            config = (files.home / "profiles/research/cordis.patch.yml").read_text()
            assert READER not in config and connection.api_key.get_secret_value() not in config
            assert str(files.identity.resolve()) in config
            assert json.loads(files.identity.read_text()) == {"token": READER}
            assert files.identity.stat().st_mode & 0o777 == 0o600
            assert files.home.stat().st_mode & 0o777 == 0o700
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())
