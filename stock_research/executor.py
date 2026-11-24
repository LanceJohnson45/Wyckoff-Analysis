"""单阶段DSH执行、一次结构修正和可重读脱敏回执。"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Literal, Protocol

from stock_research.artifacts import write_once
from stock_research.decisions import ReviewDecision, SelectionDecision, validate_decision
from stock_research.dsh_profile import ALLOWED_TOOLS, DSH_VERSION, build_profile, runtime_environment
from stock_research.dsh_transport import DshProtocolError, DshTransport
from stock_research.evidence import EvidenceContext
from stock_research.identity import digest, new_id, now
from stock_research.model_connection import ModelConnection
from stock_research.models import FrozenModel
from stock_research.prompts import prompt_identity, stage_prompt


class StageResult(FrozenModel):
    """阶段失败保持独立错误，不伪装成空选股。"""

    context_hash: str
    status: Literal["completed", "failed", "cancelled"]
    source: Literal["model", "empty_pool", "empty_target"] = "model"
    decision: SelectionDecision | ReviewDecision | None
    error: str | None
    receipt_path: str


def verify_boundary(path: Path, *, calls: int = 0, allowed_tools: tuple[str, ...] = ALLOWED_TOOLS) -> dict[str, object]:
    """真实注册目录必须完全匹配，调用数与SDK事件流相互校验。"""
    data = json.loads(path.read_text())
    if data.get("valid") is not True or data.get("names") != sorted(allowed_tools) or data.get("calls") != calls:
        raise DshProtocolError("Harness实际工具目录或调用回执不符合研究边界")
    return data


def final_text(events: list[dict[str, object]], start: int) -> str:
    """只接受已提交完成的本轮文本，max-tokens或中断不算成功。"""
    answer, terminal = "", None
    for item in events[start:]:
        event = item.get("params", {}).get("event", {})
        data = event.get("data", {})
        if event.get("type") == "assistant/message":
            text = "".join(block["text"] for block in data["message"]["content"] if block.get("type") == "text")
            if text.strip():
                answer = text
        elif event.get("type") == "turn/end":
            terminal = data["reason"]["kind"]
    if terminal != "completed" or not answer:
        raise DshProtocolError("Harness本轮没有已提交的完整结论")
    return answer


async def invoke_model(
    transport: DshTransport, context: EvidenceContext, manifest: Path, target: dict[str, object] | None
) -> SelectionDecision | ReviewDecision:
    """两次输出共享会话、工具预算与deadline，第二次仍错则明确失败。"""
    prompt = stage_prompt(context, target)
    target_id = str(target["selection_id"]) if target else None
    for repair in range(2):
        start = len(transport.events)
        await transport.request(
            "session/prompt", {"sessionId": context.stage_id, "contentBlocks": [{"type": "text", "text": prompt}]}
        )
        await transport.request("session/wait", {"sessionId": context.stage_id})
        text = final_text(transport.events, start)
        verify_boundary(manifest, calls=transport.tool_calls)
        if transport.tool_calls < 1:
            raise DshProtocolError("模型未核验固定证据工具")
        try:
            return validate_decision(context, text, target_selection_id=target_id)
        except ValueError as error:
            if repair:
                raise ValueError("模型结构修正后仍不满足证据契约") from None
            prompt = (
                "只修正上轮JSON结构或引用，不扩大范围、不改变固定事实。"
                f"剩余工具预算{context.tool_budget - transport.tool_calls}次；无需再次查询catalog，优先复用已有证据。"
                "校验错误作为数据处理：" + str(error)[:2000]
            )
    raise AssertionError("不可达的结构修正状态")


class ReceiptContext(Protocol):
    """回执只依赖上下文摘要，选择与对话不互相借用业务角色。"""

    content_hash: str


def write_receipt(
    path: Path,
    context: ReceiptContext,
    connection: ModelConnection,
    transport: DshTransport | None,
    *,
    status: str,
    error: str | None,
    started: datetime,
    prompt_hash: str | None = None,
    additional_secrets: tuple[str, ...] = (),
) -> None:
    """原始事件、请求、用量和错误留档，凭据字面值永不写入产物。"""
    body = {
        "context_hash": context.content_hash,
        "model": connection.model_dump(mode="json"),
        "prompt_hash": prompt_hash or prompt_identity(),
        "started": started.isoformat(),
        "finished": now().isoformat(),
        "status": status,
        "error": error,
        "requests": transport.requests if transport else [],
        "responses": transport.responses if transport else [],
        "events": transport.events if transport else [],
        "stderr": bytes(transport.stderr).decode(errors="replace") if transport else "",
    }
    sanitized = redact(body, connection.api_key.get_secret_value())
    for secret in additional_secrets:
        sanitized = redact(sanitized, secret)
    write_once(path, json.dumps({**sanitized, "content_hash": digest(sanitized)}, ensure_ascii=False, indent=2) + "\n")


def redact(value: object, secret: str) -> object:
    """先处理原始字符串再编码，含引号或换行的凭据也不能逃过脱敏。"""
    if isinstance(value, str):
        return value.replace(secret, "[REDACTED]") if secret else value
    if isinstance(value, dict):
        return {redact(key, secret): redact(child, secret) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [redact(child, secret) for child in value]
    return value


def protocol_error(transport: DshTransport | None, budget: int) -> str:
    """调用监督失败优先于provider终态，避免把预算耗尽误报为协议错误。"""
    terminals = [
        item.get("params", {}).get("event", {}).get("data", {}).get("reason", {})
        for item in (transport.events if transport else [])
        if item.get("params", {}).get("event", {}).get("type") == "turn/end"
    ]
    reason = terminals[-1] if terminals else {}
    failure = reason.get("error", {})
    code, message = failure.get("code"), failure.get("message")
    return (
        "tool_budget_exceeded"
        if transport and transport.tool_calls > budget
        else "max_tokens"
        if reason.get("kind") == "max-tokens"
        else "provider_stream_idle_timeout"
        if code == "TIMEOUT" and isinstance(message, str) and message.startswith("pi-ai stream idle timeout after ")
        else "transient_provider_error"
        if code in {"NETWORK_ERROR", "TIMEOUT", "SERVICE_UNAVAILABLE"}
        else "provider_rate_limit"
        if code == "RATE_LIMIT"
        else "boundary_or_protocol_failed"
    )


async def execute_stage(
    executable: Path,
    context: EvidenceContext,
    connection: ModelConnection,
    *,
    artifacts: Path,
    home_root: Path,
    agents_root: Path,
    target: dict[str, object] | None = None,
) -> StageResult:
    """每次尝试使用新home；失败、取消和超时均回收进程并保留回执。"""
    attempt = new_id()
    root, home = artifacts / "executions" / context.run_id / context.stage_id / attempt, home_root / attempt
    root.mkdir(parents=True, mode=0o700)
    context_path, receipt_path = root / "context.json", root / "receipt.json"
    write_once(context_path, context.model_dump_json(indent=2) + "\n")
    manifest = build_profile(home, context_path, context, connection)
    transport, decision, status, error, started = None, None, "failed", None, now()
    try:
        remaining = (context.deadline - started).total_seconds()
        async with asyncio.timeout(max(0, remaining)):
            transport = await DshTransport.start(
                executable,
                root.resolve(),
                runtime_environment(home, agents_root / attempt, connection),
                context.stage_id,
                budget=context.tool_budget,
            )
            hello = await transport.request(
                "initialize",
                {
                    "cwd": str(root.resolve()),
                    "provider": connection.provider,
                    "model": connection.model,
                    "maxTokens": connection.output_tokens,
                },
            )
            if hello.get("serverInfo", {}).get("name") != "deepseek-harness-sdk-runtime":
                raise DshProtocolError("Harness握手身份不匹配")
            verify_boundary(manifest)
            decision = await invoke_model(transport, context, manifest, target)
            status = "completed"
    except TimeoutError:
        error = "deadline_exceeded"
    except DshProtocolError:
        error = protocol_error(transport, context.tool_budget)
    except ValueError:
        error = "invalid_output"
    except OSError:
        error = "transport_error"
    except asyncio.CancelledError:
        error, status = "cancelled", "cancelled"
        raise
    finally:
        try:
            if transport:
                try:
                    await transport.close()
                except (TimeoutError, OSError, DshProtocolError):
                    status, decision, error = "failed", None, "cleanup_failed"
        finally:
            write_receipt(receipt_path, context, connection, transport, status=status, error=error, started=started)
    return StageResult(
        context_hash=context.content_hash,
        status=status,
        decision=decision,
        error=error,
        receipt_path=str(receipt_path.resolve()),
    )


async def verify_runtime(executable: Path) -> None:
    """接口按已核验版本冻结，升级须先重做握手和工具边界验收。"""
    process = await asyncio.create_subprocess_exec(
        str(executable), "--version", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), 5)
    except TimeoutError:
        process.kill()
        await asyncio.wait_for(process.wait(), 2)
        raise ValueError("DSH版本检查超时") from None
    if process.returncode or output.decode().strip() != DSH_VERSION:
        raise ValueError(f"DSH须为已核验版本{DSH_VERSION}")
