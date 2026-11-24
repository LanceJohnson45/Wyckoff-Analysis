"""仅注册研究工具实际返回的标量，问答引用不能解析成文件或SQL。"""

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from pathlib import Path

from mcp.server.fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field, model_validator

from stock_research.chat_projection import project_chat_data
from stock_research.decisions import same_value
from stock_research.evidence import Evidence
from stock_research.identity import digest, new_id
from stock_research.models import FrozenModel
from stock_research.query_client import QueryError
from stock_research.schemas.chat import ChatAnswer

MAX_TOOL_OUTPUT_BYTES = 48 * 1024
MAX_CHAT_OUTPUT_BYTES = 256 * 1024


class ChatToolOutput(FrozenModel):
    context_hash: str
    facts: tuple[Evidence, ...]
    total_facts: int
    next_offset: int | None


def evidence_page(context_hash: str, facts: dict[str, Evidence], offset: int) -> ChatToolOutput:
    """按UTF-8字节限制完整事实页；不截断单条事实或把未返回页登记为已读。"""
    if not 0 <= offset <= 20000:
        raise QueryError("fact_offset须在0～20000之间")
    values = list(facts.values())
    page, size = [], 512
    for fact in values[offset:]:
        length = len(fact.model_dump_json().encode()) + 1
        if size + length > MAX_TOOL_OUTPUT_BYTES:
            if not page:
                raise QueryError("单条证据过长，请改用更具体的查询")
            break
        page.append(fact)
        size += length
    end = offset + len(page)
    return ChatToolOutput(
        context_hash=context_hash,
        facts=tuple(page),
        total_facts=len(values),
        next_offset=end if end < len(values) else None,
    )


class ChatEvidence(FrozenModel):
    context_hash: str
    calls: int = Field(ge=0)
    successes: int = Field(ge=0)
    facts: dict[str, Evidence]
    content_hash: str

    @model_validator(mode="after")
    def valid_registry(self) -> "ChatEvidence":
        """注册表摘要与引用键都必须一致，失败工具不能冒充已读取证据。"""
        if self.successes > self.calls or any(key != value.ref for key, value in self.facts.items()):
            raise ValueError("问答证据注册表无效")
        if digest(self.model_dump(mode="json", exclude={"content_hash"})) != self.content_hash:
            raise ValueError("问答证据注册表摘要不一致")
        return self


def collect_chat_facts(value: object, prefix: str, result: dict[str, Evidence]) -> None:
    """原ref优先；其他返回字段使用带结果摘要的只读路径，数组仅补基数元数据。"""
    if isinstance(value, dict):
        if {"ref", "value"} <= value.keys():
            fact = Evidence(ref=value["ref"], value=value["value"], symbol=value.get("symbol"))
            result[fact.ref] = fact
            return
        ref_prefix = value.get("ref_prefix", prefix)
        for key, child in value.items():
            if key != "ref_prefix":
                collect_chat_facts(child, f"{ref_prefix}/{key}", result)
    elif isinstance(value, list):
        result[prefix + "/count"] = Evidence(ref=prefix + "/count", symbol=None, value=len(value))
        for index, child in enumerate(value):
            collect_chat_facts(child, f"{prefix}/{index}", result)
    else:
        result[prefix] = Evidence(ref=prefix, symbol=None, value=value)


class ChatAudit:
    """一个工具进程拥有一个上下文；并行工具结果串行提交证据注册表。"""

    def __init__(self, context_hash: str, path: Path) -> None:
        self.context_hash, self.path = context_hash, path
        self.calls, self.successes = 0, 0
        self.output_bytes = 0
        self.facts: dict[str, Evidence] = {}
        self.lock = asyncio.Lock()

    def persist(self) -> None:
        """原子替换已提交注册表，模型进程退出时不会留下半份JSON。"""
        body = {
            "context_hash": self.context_hash,
            "calls": self.calls,
            "successes": self.successes,
            "facts": {key: value.model_dump(mode="json") for key, value in self.facts.items()},
        }
        temporary = self.path.with_name(self.path.name + "." + new_id())
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as output:
                output.write(json.dumps({**body, "content_hash": digest(body)}, ensure_ascii=False))
            temporary.replace(self.path)
        finally:
            temporary.unlink(missing_ok=True)

    async def respond(
        self, tool: str, operation: Callable[[], Awaitable[BaseModel]], *, fact_offset: int = 0
    ) -> ChatToolOutput:
        """工具错误不输出连接详情，只有成功且有界的返回值可进入回答引用。"""
        async with self.lock:
            self.calls += 1
            await asyncio.to_thread(self.persist)
        try:
            data = project_chat_data(tool, (await operation()).model_dump(mode="json"))
            facts = {}
            collect_chat_facts(data, f"read/{tool}/{digest(data)[:16]}", facts)
            if len(facts) > 20000:
                raise QueryError("查询证据过多，请缩小历史或分页范围")
            output = evidence_page(self.context_hash, facts, fact_offset)
            returned = {fact.ref: fact for fact in output.facts}
            size = len(output.model_dump_json().encode())
            async with self.lock:
                if self.output_bytes + size > MAX_CHAT_OUTPUT_BYTES:
                    raise QueryError("本轮已读取足够证据，请根据已返回事实作答或另起问题")
                if any(
                    ref in self.facts and not same_value(fact.value, self.facts[ref].value)
                    for ref, fact in returned.items()
                ):
                    raise QueryError("固定证据引用出现不一致")
                self.facts.update(returned)
                self.output_bytes += size
                self.successes += 1
                await asyncio.to_thread(self.persist)
            return output
        except QueryError as error:
            raise ToolError(str(error)) from None


def validate_chat_answer(text: str, evidence: ChatEvidence, context_hash: str) -> ChatAnswer:
    """观点必须有已读引用，数字/布尔逐值核验，纯文字立场仍是模型观点。"""
    answer = ChatAnswer.model_validate_json(text)
    if answer.context_hash != context_hash or evidence.context_hash != context_hash or evidence.successes < 1:
        raise ValueError("问答没有核验本轮固定证据")
    for point in (answer.answer, *answer.observations, *answer.risks, *answer.confirmation_conditions):
        if any(ref not in evidence.facts for ref in point.evidence_refs):
            raise ValueError("观点引用不属于本轮已读证据")
    for claim in answer.claims:
        if claim.ref not in evidence.facts or not same_value(claim.value, evidence.facts[claim.ref].value):
            raise ValueError("问答声明与实际工具证据不一致")
    return answer
