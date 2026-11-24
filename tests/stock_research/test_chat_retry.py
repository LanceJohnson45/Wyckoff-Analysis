"""原生PG与实际stdio子进程验证重试；Harness及证据为明确合成测试数据。"""

import asyncio
import json
import os
import sys
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from stock_research.chat_manager import ChatManager
from stock_research.chat_store import ChatStore
from stock_research.config import Settings
from stock_research.errors import ResearchConflict, ResearchNotFound
from stock_research.http_api import create_app
from stock_research.identity import digest
from stock_research.model_connection import ModelConnection
from stock_research.models import Snapshot
from stock_research.run_store import RunStore
from stock_research.schema import CHAT_TURNS
from stock_research.schemas.chat import ChatContext, ChatResult, CreateChatTurn
from stock_research.store import close_engine, database_engine
from tests.stock_research.test_chat import chat_report
from tests.stock_research.test_chat import chat_settings as chat_settings
from tests.stock_research.test_queries import OPERATOR, READER

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


@pytest.fixture
def controlled_harness(tmp_path: Path) -> Path:
    """协议与监督使用真实进程，供应商和工具证据替换为有标识的合成数据。"""
    executable = tmp_path / "controlled-chat-harness"
    executable.write_text(
        f"#!{sys.executable}\n"
        + r"""
import hashlib,json,os,sys
from pathlib import Path
if "--version" in sys.argv:
    print("0.2.1-alpha.2")
    raise SystemExit(0)
root=Path.cwd()
context=json.loads((root/"context.json").read_text())
(root/"owned-pid").write_text(str(os.getpid()))
model=None
patch=json.loads((Path(os.environ["DSH_HOME"])/"profiles/research/cordis.patch.yml").read_text())
boundary=next(item["config"] for group in patch for item in group.get("insert",[]) if item.get("id")=="research-boundary")
def manifest(calls):
    Path(boundary["manifest"]).write_text(json.dumps({"names":sorted(boundary["allowedTools"]),"valid":True,"calls":calls}))
def emit(value):
    print(json.dumps(value),flush=True)
def event(kind,data):
    emit({"jsonrpc":"2.0","method":"session/event","params":{"sessionId":context["stage_id"],"event":{"type":kind,"data":data}}})
for line in sys.stdin:
    packet=json.loads(line)
    method=packet["method"]
    if method=="initialize":
        model=packet["params"]["model"]
        if model!="fixture-no-manifest":
            manifest(0)
        result={"serverInfo":{"name":"deepseek-harness-sdk-runtime"}}
    else:
        result={}
    if method=="session/prompt":
        count=3 if context["tool_budget"]==20 else 1
        for _ in range(count):
            event("tool/call",{"name":"mcp__chat__report_read"})
        manifest(count)
        (root/"prompt-started").touch()
    if method=="session/wait":
        if context["tool_budget"]==20 and model!="fixture-no-evidence":
            event("turn/end",{"reason":{"kind":"error","error":{"code":"SERVICE_UNAVAILABLE"}}})
        elif model=="fixture-hang":
            continue
        else:
            body={"context_hash":context["content_hash"],"calls":1,"successes":1,"facts":{"fixture/value":{"ref":"fixture/value","value":11.0,"symbol":None}}}
            canonical=json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(",",":"),allow_nan=False)
            body["content_hash"]=hashlib.sha256(canonical.encode()).hexdigest()
            if model!="fixture-no-evidence":
                (root/"chat-evidence.json").write_text(json.dumps(body))
            answer={"context_hash":context["content_hash"],"answer":{"text":"固定合成观点","evidence_refs":["fixture/value"]},"observations":[],"risks":[],"confirmation_conditions":[],"claims":[{"ref":"fixture/value","value":11}]}
            event("assistant/message",{"message":{"content":[{"type":"text","text":json.dumps(answer)}]}})
            event("turn/end",{"reason":{"kind":"completed"}})
    emit({"jsonrpc":"2.0","id":packet["id"],"result":result})
    if method=="shutdown":
        break
"""
    )
    executable.chmod(0o700)
    return executable


def model_connection(model: str = "fixture") -> ModelConnection:
    return ModelConnection(provider="fixture", model=model, base_url="https://example.invalid", api_key="fixture-key")


def test_retry_actual_harness_preserves_attempts_and_http_replay(
    chat_settings: Settings, snapshot: Snapshot, controlled_harness: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """真实受理/执行/回收/独立重读，重试不改模型、截止或原问题。"""

    async def scenario() -> None:
        settings = chat_settings.model_copy(update={"dsh_executable": controlled_harness})
        store, report_id = await chat_report(settings, snapshot)
        model_calls = []

        def controlled_model(_settings: Settings) -> ModelConnection:
            model_calls.append(True)
            return model_connection()

        monkeypatch.setattr("stock_research.chat_manager.primary_model", controlled_model)
        try:
            app = create_app(settings)
            headers = {"Authorization": "Bearer " + OPERATOR, "Idempotency-Key": "fixture-actual-chat-retry"}
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                body = {"report_id": report_id, "question": "固定问题，临时失败重试"}
                created = await client.post("/v1/chat-turns", json=body)
                assert created.status_code == 202
                identifier = created.json()["data"]["id"]
                async with asyncio.timeout(10):
                    while (view := await ChatStore(store).read(identifier)).status in {"pending", "running"}:
                        await asyncio.sleep(0.01)
                assert view.status == "completed", view
                assert len(model_calls) == 1 and view.tool_calls == 4
                assert [attempt.status for attempt in view.attempts] == ["failed", "completed"]
                assert [attempt.tool_budget for attempt in view.attempts] == [20, 17]
                assert view.attempts[0].deadline == view.attempts[1].deadline
                assert view.attempts[0].context_hash != view.attempts[1].context_hash == view.answer.context_hash
                public = (await client.get(f"/v1/chat-turns/{identifier}")).json()["data"]
                assert public["tool_calls"] == 4 and len(public["attempts"]) == 2
                assert "receipt_path" not in json.dumps(public) and "fixture-key" not in json.dumps(public)
                assert (await client.post("/v1/chat-turns", json=body)).json()["data"]["id"] == identifier
                assert len(model_calls) == 1
                assert (
                    await client.get(f"/v1/chat-turns/{identifier}", headers={"Authorization": "Bearer " + READER})
                ).status_code == 403
                foreign = ChatStore(RunStore(store.engine, "foreign-workspace"))
                with pytest.raises(ResearchNotFound):
                    await foreign.read(identifier)
            independent = database_engine(settings)
            try:
                assert await ChatStore(RunStore(independent, settings.workspace)).read(identifier) == view
                async with independent.connect() as connection:
                    payload = await connection.scalar(
                        select(CHAT_TURNS.c.payload).where(CHAT_TURNS.c.uuid == identifier)
                    )
                receipts = [Path(attempt["receipt_path"]) for attempt in payload["attempts"]]
                assert receipts[0].parent != receipts[1].parent
                assert [json.loads(path.read_text())["status"] for path in receipts] == ["failed", "completed"]
                contexts = [
                    ChatContext.model_validate_json((path.parent / "context.json").read_text()) for path in receipts
                ]
                assert contexts[0].report_id == contexts[1].report_id == view.report_id
                assert contexts[0].question == contexts[1].question == view.question
                for path in receipts:
                    assert path.stat().st_mode & 0o777 == 0o600
                    pid = int((path.parent / "owned-pid").read_text())
                    with pytest.raises(ProcessLookupError):
                        os.kill(pid, 0)
                identities = list(settings.dsh_home.glob(f"chat/{identifier}/*/query-identity.json"))
                assert identities == []
            finally:
                await close_engine(independent)
        finally:
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("error", "calls", "expected"),
    [
        ("transient_provider_error", 3, 2),
        ("provider_stream_idle_timeout", 3, 2),
        ("transport_error", 3, 2),
        ("transient_provider_error", 20, 1),
    ]
    + [
        (error, 1, 1)
        for error in (
            "provider_rate_limit",
            "invalid_output",
            "max_tokens",
            "tool_budget_exceeded",
            "boundary_or_protocol_failed",
            "cleanup_failed",
            "deadline_exceeded",
        )
    ],
)
def test_retry_only_transient_with_remaining_budget(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch, error: str, calls: int, expected: int
) -> None:
    """受控传输覆盖失败分类和次数；两次失败仍只有原问题及原终态。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        contexts = []

        async def failed(_settings: Settings, context: ChatContext, _connection: ModelConnection) -> ChatResult:
            contexts.append(context)
            return ChatResult(
                status="failed",
                answer=None,
                error_code=error,
                tool_calls=calls,
                context_hash=context.content_hash,
                receipt_path="synthetic-receipt",
            )

        monkeypatch.setattr("stock_research.chat_manager.primary_model", lambda _settings: model_connection())
        monkeypatch.setattr("stock_research.chat_manager.execute_chat", failed)
        manager = ChatManager(store, chat_settings)
        try:
            turn = await manager.submit(
                "fixture-failure-classification", CreateChatTurn(report_id=report_id, question="分类验证")
            )
            await asyncio.wait_for(asyncio.gather(*manager.tasks.values()), 5)
            view = await manager.chats.read(str(turn.id))
            assert len(contexts) == len(view.attempts) == expected
            assert view.status == "failed" and view.error_code == error and view.answer is None
            assert view.tool_calls == calls * expected
            if expected == 2:
                assert contexts[1].tool_budget == 20 - calls and contexts[1].deadline == contexts[0].deadline
            assert (
                await manager.submit(
                    "fixture-failure-classification", CreateChatTurn(report_id=report_id, question="分类验证")
                )
            ).id == turn.id
            assert len(contexts) == expected
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())


def test_cancel_second_attempt_reaps_process_and_restart_does_not_replay(
    chat_settings: Settings, snapshot: Snapshot, controlled_harness: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """实际第二进程等待时取消；原失败回执保留，未知次数不补零。"""

    async def scenario() -> None:
        settings = chat_settings.model_copy(update={"dsh_executable": controlled_harness})
        store, report_id = await chat_report(settings, snapshot)
        monkeypatch.setattr(
            "stock_research.chat_manager.primary_model", lambda _settings: model_connection("fixture-hang")
        )
        manager = ChatManager(store, settings)
        try:
            turn = await manager.submit(
                "fixture-cancel-retry", CreateChatTurn(report_id=report_id, question="第二次运行取消")
            )
            async with asyncio.timeout(10):
                while len(list(settings.artifacts_path.glob(f"chat-executions/{turn.id}/*/prompt-started"))) < 2:
                    await asyncio.sleep(0.01)
            cancelled = await manager.cancel(str(turn.id))
            assert cancelled.status == "cancelled" and cancelled.tool_calls == 3
            assert [item.status for item in cancelled.attempts] == ["failed", "cancelled"]
            assert cancelled.attempts[1].tool_calls is None
            for marker in settings.artifacts_path.glob(f"chat-executions/{turn.id}/*/owned-pid"):
                with pytest.raises(ProcessLookupError):
                    os.kill(int(marker.read_text()), 0)
            pending = await manager.chats.create(
                "fixture-restart-unknown", CreateChatTurn(report_id=report_id, question="中断不重放")
            )
            await manager.chats.transition(str(pending.id), "running", model="fixture")
            await manager.chats.start_attempt(str(pending.id), await manager.context(pending))
            await ChatManager(store, settings).restore()
            interrupted = await manager.chats.read(str(pending.id))
            assert interrupted.status == "failed" and interrupted.error_code == "execution_interrupted"
            assert (
                interrupted.attempts[0].error_code == "execution_interrupted"
                and interrupted.attempts[0].tool_calls is None
            )
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())


def test_attempt_compare_and_swap_rejects_stale_running_update(chat_settings: Settings, snapshot: Snapshot) -> None:
    """同running状态下也比较摘要，迟到推进不能覆盖新尝试。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        manager = ChatManager(store, chat_settings)
        try:
            turn = await manager.chats.create(
                "fixture-attempt-cas", CreateChatTurn(report_id=report_id, question="并发尝试")
            )
            await manager.chats.transition(str(turn.id), "running", model="fixture")
            before = await store.row(CHAT_TURNS, str(turn.id))
            context = await manager.context(turn)
            outcomes = await asyncio.gather(
                manager.chats.start_attempt(str(turn.id), context),
                manager.chats.start_attempt(str(turn.id), context),
                return_exceptions=True,
            )
            assert sum(isinstance(item, ResearchConflict) for item in outcomes) == 1
            with pytest.raises(ResearchConflict):
                await manager.chats.write_payload(before, before["payload"])
            current = await manager.chats.read(str(turn.id))
            assert len(current.attempts) == 1 and current.attempts[0].context_hash == context.content_hash
            assert digest((await store.row(CHAT_TURNS, str(turn.id)))["payload"]) != before["content_hash"]
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("expire_before", [True, False])
def test_original_deadline_never_renews_for_retry(
    chat_settings: Settings, snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch, expire_before: bool
) -> None:
    """只推进判断时钟，不等待真实provider；既有截止后不能启动下一次。"""

    async def scenario() -> None:
        store, report_id = await chat_report(chat_settings, snapshot)
        manager, calls = ChatManager(store, chat_settings), []

        async def expired(_settings: Settings, context: ChatContext, _connection: ModelConnection) -> ChatResult:
            calls.append(context)
            monkeypatch.setattr("stock_research.chat_manager.now", lambda: context.deadline)
            return ChatResult(
                status="failed",
                answer=None,
                error_code="transient_provider_error",
                tool_calls=1,
                context_hash=context.content_hash,
                receipt_path="synthetic-expired-receipt",
            )

        monkeypatch.setattr("stock_research.chat_manager.primary_model", lambda _settings: model_connection())
        monkeypatch.setattr("stock_research.chat_manager.execute_chat", expired)
        try:
            turn = await manager.chats.create(
                "fixture-original-deadline", CreateChatTurn(report_id=report_id, question="原期限")
            )
            if expire_before:
                context = await manager.context(turn)
                monkeypatch.setattr("stock_research.chat_manager.now", lambda: context.deadline)
            await manager.run(str(turn.id))
            view = await manager.chats.read(str(turn.id))
            assert view.status == "failed" and len(calls) == (0 if expire_before else 1)
            assert len(view.attempts) == len(calls)
            assert view.error_code == ("deadline_exceeded" if expire_before else "transient_provider_error")
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("missing", ["manifest", "evidence", "executable"])
def test_local_artifact_and_executable_failures_never_retry_model(
    chat_settings: Settings, snapshot: Snapshot, controlled_harness: Path, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    """真实本地文件失败不能冒充网络抖动，避免重复执行或模型消费。"""

    async def scenario() -> None:
        executable = (
            controlled_harness.with_name("nonexistent-executable") if missing == "executable" else controlled_harness
        )
        settings = chat_settings.model_copy(update={"dsh_executable": executable})
        store, report_id = await chat_report(settings, snapshot)
        monkeypatch.setattr(
            "stock_research.chat_manager.primary_model", lambda _settings: model_connection("fixture-no-" + missing)
        )
        manager = ChatManager(store, settings)
        try:
            turn = await manager.submit(
                "fixture-local-unavailable", CreateChatTurn(report_id=report_id, question="本地边界文件损坏")
            )
            await asyncio.wait_for(asyncio.gather(*manager.tasks.values()), 10)
            view = await manager.chats.read(str(turn.id))
            assert view.status == "failed" and len(view.attempts) == 1
            assert view.error_code == (
                "executor_unavailable" if missing == "executable" else "boundary_or_protocol_failed"
            )
            assert view.tool_calls == (3 if missing == "evidence" else 0)
            for marker in settings.artifacts_path.glob(f"chat-executions/{turn.id}/*/owned-pid"):
                with pytest.raises(ProcessLookupError):
                    os.kill(int(marker.read_text()), 0)
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())


def test_committed_answer_survives_owner_cancellation(
    chat_settings: Settings, snapshot: Snapshot, controlled_harness: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """在成功尝试提交后立即打断拥有者，回答仍须原子存在且重放不重新消费。"""

    async def scenario() -> None:
        settings = chat_settings.model_copy(update={"dsh_executable": controlled_harness})
        store, report_id = await chat_report(settings, snapshot)
        monkeypatch.setattr("stock_research.chat_manager.primary_model", lambda _settings: model_connection())
        finish = ChatStore.finish_attempt

        async def interrupted(self: ChatStore, identifier: str, result: ChatResult):
            view = await finish(self, identifier, result)
            if result.status == "completed":
                raise asyncio.CancelledError
            return view

        monkeypatch.setattr(ChatStore, "finish_attempt", interrupted)
        manager = ChatManager(store, settings)
        request = CreateChatTurn(report_id=report_id, question="成功提交后拥有者中断")
        try:
            turn = await manager.submit("fixture-committed-answer", request)
            outcome = await asyncio.wait_for(asyncio.gather(*manager.tasks.values(), return_exceptions=True), 10)
            assert len(outcome) == 1 and isinstance(outcome[0], asyncio.CancelledError)
            view = await manager.chats.read(str(turn.id))
            assert view.status == "completed" and view.answer is not None and view.error_code is None
            assert (
                view.attempts[-1].status == "completed" and view.answer.context_hash == view.attempts[-1].context_hash
            )
            assert view.tool_calls == 4 and len(view.attempts) == 2
            assert await manager.submit("fixture-committed-answer", request) == view
            assert not manager.tasks
        finally:
            await manager.close()
            await close_engine(store.engine)

    asyncio.run(scenario())
