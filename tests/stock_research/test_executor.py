"""真实stdio证据工具、SDK握手及失控子进程回收；不调用外部模型。"""

import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from stock_research.dsh_profile import ALLOWED_TOOLS, build_profile, runtime_environment
from stock_research.dsh_transport import DshProtocolError, DshTransport
from stock_research.executor import final_text, protocol_error, verify_boundary, verify_runtime, write_receipt
from stock_research.identity import digest, now
from stock_research.model_connection import ModelConnection, load_model_connection, load_profile_connection
from tests.stock_research.test_evidence import make_context


def connection() -> ModelConnection:
    return ModelConnection(
        provider="fixture", model="fixture", base_url="https://example.invalid/v1", api_key="fixture-key"
    )


def test_mcp_stdio_lists_exactly_two_tools_and_denies_foreign_stock(tmp_path: Path) -> None:
    """实际MCP传输验证工具边界与证据值；子进程不继承行情及网关凭据。"""
    context = make_context()
    path = tmp_path / "context.json"
    path.write_text(context.model_dump_json())

    async def scenario() -> None:
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "stock_research.evidence_mcp"],
            env={"STOCK_RESEARCH_CONTEXT_FILE": str(path), "PYTHONPATH": str(Path.cwd())},
        )
        async with (
            asyncio.timeout(15),
            stdio_client(parameters) as (read, write),
            ClientSession(read, write) as session,
        ):
            await session.initialize()
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == {"evidence", "history"}
            fact = await session.call_tool("evidence", {"ref": "martin/AAPL/factors/close"})
            assert not fact.isError
            assert json.loads(fact.content[0].text)["value"] == 11
            refused = await session.call_tool("evidence", {"ref": "stock/MSFT"})
            assert refused.isError

    asyncio.run(scenario())


def test_profile_extracts_only_static_registered_model(tmp_path: Path) -> None:
    profile, credentials = tmp_path / "profile.yml", tmp_path / "credentials.yml"
    profile.write_text("""
- id: agent-default-model
  config: {provider: fixture, model: small}
- id: provider
  config:
    providers:
      fixture:
        api: openai-completions
        baseURL: https://example.invalid/v1
        apiKeyEnv: FIXTURE_KEY
        models: [{id: small}, {id: large}]
- id: unsafe-plugin
  config: !!js |
    throw new Error("never execute")
""")
    credentials.write_text('refs: {FIXTURE_KEY: "fixture-secret"}\n')
    result = load_profile_connection(profile, credentials)
    assert result.model == "small"
    assert result.available_models == ("small", "large")
    assert "fixture-secret" not in result.model_dump_json()
    with pytest.raises(ValueError, match="注册"):
        load_profile_connection(profile, credentials, model="unregistered")


def test_shell_connection_is_operator_owned_and_environment_is_whitelisted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "trusted.env"
    path.write_text("""DSH_E2E_MODEL_PROVIDER=fixture
DSH_E2E_MODEL_ID=small
DSH_E2E_MODEL_STRONG_ID=large
DSH_E2E_MODEL_BASE_URL=https://example.invalid/v1
DSH_E2E_MODEL_API_KEY=$(printf fixture-secret)
""")
    result = load_model_connection(path)
    monkeypatch.setenv("PG_PASSWORD", "must-not-leak")
    environment = runtime_environment(tmp_path / "home", tmp_path / "agents", result)
    assert "PG_PASSWORD" not in environment
    assert environment["RESEARCH_MODEL_API_KEY"] == "fixture-secret"
    assert result.model == "small"


def test_receipt_redacts_secret_before_json_escaping(tmp_path: Path) -> None:
    model = connection().model_copy(update={"api_key": connection().api_key.__class__('fixture"secret\nvalue')})
    process = type(
        "Transcript",
        (),
        {"requests": [], "responses": [], "events": [], "stderr": bytearray(model.api_key.get_secret_value().encode())},
    )()
    path = tmp_path / "receipt.json"
    write_receipt(path, make_context(), model, process, status="failed", error="fixture", started=now())
    body = json.loads(path.read_text())
    content_hash = body.pop("content_hash")
    assert body["stderr"] == "[REDACTED]"
    assert content_hash == digest(body)
    assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("reason", ["max-tokens", "cancelled", "error"])
def test_incomplete_terminal_never_commits_partial_text(reason: str) -> None:
    events = [
        {
            "params": {
                "event": {
                    "type": "assistant/message",
                    "data": {"message": {"content": [{"type": "text", "text": "partial"}]}},
                }
            }
        },
        {"params": {"event": {"type": "turn/end", "data": {"reason": {"kind": reason}}}}},
    ]
    with pytest.raises(DshProtocolError):
        final_text(events, 0)


def test_protocol_failure_and_cancellation_reap_owned_process(tmp_path: Path) -> None:
    executable = tmp_path / "controlled-harness"
    executable.write_text(
        f'#!{sys.executable}\nimport sys,time\nsys.stdin.readline()\nprint("not JSON",flush=True)\ntime.sleep(60)\n'
    )
    executable.chmod(0o700)

    async def scenario() -> None:
        transport = await DshTransport.start(executable, tmp_path, {"PATH": os.environ["PATH"]}, "fixture")
        with pytest.raises(DshProtocolError):
            await transport.request("initialize")
        await transport.close()
        assert transport.process.returncode is not None
        with pytest.raises(ProcessLookupError):
            os.kill(transport.process.pid, 0)
        assert transport.stderr_task.done()

    asyncio.run(scenario())


def test_installed_dsh_offline_handshake_has_exact_tool_boundary(tmp_path: Path) -> None:
    """本机已安装的实际SDK离线握手；CI无DSH时明确跳过，不冒充模型验收。"""
    executable = Path(os.environ.get("PYTEST_DSH_EXECUTABLE", str(Path.home() / "tools/e2e/bin/dsh")))
    if not executable.is_file():
        pytest.skip("未安装已核验DSH；设置PYTEST_DSH_EXECUTABLE可执行真实离线SDK集成测试")
    context, model = make_context(), connection()
    path = tmp_path / "context.json"
    path.write_text(context.model_dump_json())
    home = tmp_path / "private-home"
    manifest = build_profile(home, path, context, model)

    async def scenario() -> None:
        await verify_runtime(executable)
        transport = await DshTransport.start(
            executable, tmp_path, runtime_environment(home, tmp_path / "agents", model), context.stage_id
        )
        try:
            async with asyncio.timeout(30):
                result = await transport.request(
                    "initialize", {"cwd": str(tmp_path), "provider": model.provider, "model": model.model}
                )
                assert result["serverInfo"]["name"] == "deepseek-harness-sdk-runtime"
                assert verify_boundary(manifest)["names"] == sorted(ALLOWED_TOOLS)
        finally:
            await transport.close()

    asyncio.run(scenario())


def test_transport_enforces_stage_budget_below_global_maximum(tmp_path: Path) -> None:
    """自定义阶段预算也由SDK事件监督，不能仅靠工具返回预算错误。"""
    frame = {
        "jsonrpc": "2.0",
        "method": "session/event",
        "params": {"sessionId": "fixture", "event": {"type": "tool/call", "data": {"name": ALLOWED_TOOLS[0]}}},
    }
    executable = tmp_path / "controlled-budget-harness"
    executable.write_text(
        f"#!{sys.executable}\nimport sys,time\nsys.stdin.readline()\nprint({json.dumps(json.dumps(frame))},flush=True)\nprint({json.dumps(json.dumps(frame))},flush=True)\ntime.sleep(60)\n"
    )
    executable.chmod(0o700)

    async def scenario() -> None:
        transport = await DshTransport.start(executable, tmp_path, {"PATH": os.environ["PATH"]}, "fixture", budget=1)
        try:
            with pytest.raises(DshProtocolError, match="超限"):
                await transport.request("initialize")
            assert transport.tool_calls == 2
            assert protocol_error(transport, 1) == "tool_budget_exceeded"
        finally:
            await transport.close()
        assert transport.process.returncode is not None

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "code,message,expected",
    [
        pytest.param(
            "TIMEOUT", "pi-ai stream idle timeout after 60000ms", "provider_stream_idle_timeout", id="error-stream-idle"
        ),
        pytest.param("TIMEOUT", "HTTP request timed out", "transient_provider_error", id="error-request-timeout"),
        pytest.param("NETWORK_ERROR", "network unavailable", "transient_provider_error", id="error-network"),
        pytest.param("RATE_LIMIT", "limited", "provider_rate_limit", id="error-rate-limit"),
    ],
)
def test_error_provider_timeout_retains_specific_stream_cause(code: str, message: str, expected: str) -> None:
    """【异常值】真实SDK终态形状区分流空闲与其他超时，额度错误仍独立。"""
    transcript = SimpleNamespace(
        tool_calls=0,
        events=[
            {
                "params": {
                    "event": {
                        "type": "turn/end",
                        "data": {"reason": {"kind": "error", "error": {"code": code, "message": message}}},
                    }
                }
            }
        ],
    )

    assert protocol_error(transcript, 20) == expected
