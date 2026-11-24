"""研究问答输入、冻结执行上下文与逐值引用回答。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, field_validator, model_validator

from stock_research.decisions import EvidenceClaim, ResearchPoint
from stock_research.errors import RETRYABLE_MODEL_ERRORS
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
    tool_calls: int = Field(ge=0)
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


class ChatAttemptView(ApiModel):
    number: int = Field(ge=1, le=2)
    status: Literal["running", "completed", "failed", "cancelled"]
    error_code: str | None
    tool_calls: int | None = Field(ge=0)
    tool_budget: int = Field(ge=1, le=20)
    context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    deadline: AwareDatetime
    started_time: AwareDatetime
    finished_time: AwareDatetime | None
    receipt_available: bool

    @model_validator(mode="after")
    def consistent_attempt(self) -> "ChatAttemptView":
        """未收尾不是失败，已失败不能抹去原因；重试必须用剩余预算。"""
        if (self.status == "running") != (self.finished_time is None):
            raise ValueError("问答尝试终态时间不一致")
        if (self.status in {"failed", "cancelled"}) != bool(self.error_code):
            raise ValueError("问答尝试错误不一致")
        if self.finished_time and self.finished_time < self.started_time:
            raise ValueError("问答尝试时间倒退")
        if self.status == "completed" and (
            not self.receipt_available or self.tool_calls is None or self.tool_calls > self.tool_budget
        ):
            raise ValueError("问答尝试完成缺少有效回执")
        return self


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
    attempts: tuple[ChatAttemptView, ...] = Field(default=(), max_length=2)
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
        if self.attempts:
            self.verify_attempts()
        return self

    def verify_attempts(self) -> None:
        """旧问答未记录尝试；新记录逐次核对累计调用及共同期限。"""
        if self.status == "pending":
            raise ValueError("未开始问答不能已有执行尝试")
        spent = 0
        for number, attempt in enumerate(self.attempts, 1):
            if attempt.number != number or attempt.tool_budget != 20 - spent:
                raise ValueError("问答尝试顺序或预算不一致")
            if attempt.deadline != self.attempts[0].deadline:
                raise ValueError("问答重试改变截止时间")
            if number < len(self.attempts) and (
                attempt.status != "failed"
                or attempt.tool_calls is None
                or attempt.error_code not in RETRYABLE_MODEL_ERRORS
            ):
                raise ValueError("问答错误不允许重试")
            spent += attempt.tool_calls or 0
        if self.tool_calls != spent or (self.status != "running" and self.attempts[-1].status == "running"):
            raise ValueError("问答状态或累计调用不一致")
        if self.status == "completed" and (
            self.attempts[-1].status != "completed" or self.context_hash != self.attempts[-1].context_hash
        ):
            raise ValueError("问答答案未绑定最后一次尝试")


class ChatTurnPage(ApiModel):
    items: tuple[ChatTurnView, ...]
    total: int
    page: int
    page_size: int
