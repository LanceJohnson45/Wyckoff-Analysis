"""研究问答输入、冻结执行上下文与逐值引用回答。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, field_validator, model_validator

from stock_research.decisions import EvidenceClaim, ResearchPoint
from stock_research.identity import digest
from stock_research.models import FrozenModel
from stock_research.schemas.api import ApiModel


class ChatAnswer(FrozenModel):
    context_hash: str
    answer: ResearchPoint
    observations: tuple[ResearchPoint, ...] = Field(max_length=6)
    risks: tuple[ResearchPoint, ...] = Field(max_length=4)
    confirmation_conditions: tuple[ResearchPoint, ...] = Field(max_length=4)
    claims: tuple[EvidenceClaim, ...] = Field(min_length=1, max_length=20)


class ChatHistory(FrozenModel):
    question: str
    status: Literal["completed", "failed", "cancelled"]
    answer: ChatAnswer | None


class ChatContext(FrozenModel):
    workspace: str
    run_id: UUID4
    report_id: UUID4
    stage_id: UUID4
    snapshot_id: UUID4
    snapshot_hash: str
    as_of: date
    target_session: date
    question: str = Field(min_length=1, max_length=6000)
    history: tuple[ChatHistory, ...] = Field(max_length=6)
    deadline: AwareDatetime
    tool_budget: int = Field(default=20, ge=1, le=20)
    api_port: int = Field(ge=1, le=65535)
    content_hash: str

    @model_validator(mode="after")
    def verify_hash(self) -> "ChatContext":
        """工具进程不能接受被替换的报告或问答上下文。"""
        if digest(self.model_dump(mode="json", exclude={"content_hash"})) != self.content_hash:
            raise ValueError("研究问答上下文摘要不一致")
        return self


class CreateChatTurn(ApiModel):
    report_id: UUID4
    question: str = Field(min_length=1, max_length=6000)
    parent_turn_id: UUID4 | None = None

    @field_validator("question")
    @classmethod
    def meaningful_question(cls, value: str) -> str:
        """空白和NUL不进入持久问答，其他用户文字不解释为工具参数。"""
        if not value.strip() or "\x00" in value:
            raise ValueError("研究问题不能为空或包含NUL")
        value.encode("utf-8")
        return value.strip()


class ChatResult(FrozenModel):
    status: Literal["completed", "failed", "cancelled"]
    answer: ChatAnswer | None
    error_code: str | None
    tool_calls: int
    context_hash: str
    receipt_path: str

    @model_validator(mode="after")
    def consistent_result(self) -> "ChatResult":
        """失败不能夹带未验证答案，成功不能缺少已验证答案。"""
        if (self.status == "completed") != (self.answer is not None) or (self.status != "completed") != bool(
            self.error_code
        ):
            raise ValueError("研究问答结果状态不一致")
        return self


ChatStatus = Literal["pending", "running", "completed", "failed", "cancelled"]


class ChatTurnView(ApiModel):
    id: UUID4
    conversation_id: UUID4
    parent_turn_id: UUID4 | None
    revision: int = Field(ge=1)
    report_id: UUID4
    question: str
    status: ChatStatus
    answer: ChatAnswer | None
    error_code: str | None
    model: str | None
    context_hash: str | None
    source_hash: str
    prompt_hash: str
    tool_calls: int = Field(ge=0)
    receipt_available: bool
    create_time: AwareDatetime
    update_time: AwareDatetime

    @model_validator(mode="after")
    def consistent_turn(self) -> "ChatTurnView":
        """公开状态不能把未验证回答当成功，也不把失败隐藏成空消息。"""
        if (self.status == "completed") != (self.answer is not None):
            raise ValueError("问答答案与状态不一致")
        if (self.status in {"failed", "cancelled"}) != bool(self.error_code):
            raise ValueError("问答错误与状态不一致")
        if (self.revision == 1) != (self.parent_turn_id is None) or (
            self.parent_turn_id is None and self.conversation_id != self.id
        ):
            raise ValueError("问答根或顺序不一致")
        if self.status == "completed" and (
            not self.model
            or not self.context_hash
            or not self.receipt_available
            or self.answer.context_hash != self.context_hash
        ):
            raise ValueError("问答完成缺少执行回执")
        return self


class ChatTurnPage(ApiModel):
    items: tuple[ChatTurnView, ...]
    total: int
    page: int
    page_size: int
