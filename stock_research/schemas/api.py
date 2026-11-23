"""研究API只公开业务数据，不泄露模型连接、凭据或本地文件路径。"""

from datetime import date
from enum import StrEnum
from typing import Generic, Literal, TypeVar

from pydantic import UUID4, AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from stock_research.decisions import ReviewDecision, SelectionDecision
from stock_research.models import QualityReport

RunStatus = Literal["pending", "running", "completed", "partial", "failed", "cancelled"]
Strategy = Literal["martin", "wyckoff"]
Payload = TypeVar("Payload")


class Code(StrEnum):
    """稳定错误族；HTTP状态如实表达请求失败。"""

    OK = "0000"
    UNAUTHENTICATED = "research.unauthenticated"
    FORBIDDEN = "research.forbidden"
    NOT_FOUND = "research.not_found"
    CONFLICT = "research.conflict"
    INVALID = "research.invalid_input"
    UNAVAILABLE = "research.unavailable"
    INTERNAL = "research.internal_error"


class ApiModel(BaseModel):
    """对外字段白名单及未知输入拒收。"""

    model_config = ConfigDict(extra="forbid")


class ApiResponse(ApiModel, Generic[Payload]):
    """成功与错误统一四键，data类型由具体端点声明。"""

    code: Code
    message: str
    data: Payload | None
    correlation_id: UUID4


class HealthView(ApiModel):
    """存活不代表研究数据覆盖或模型可用。"""

    status: Literal["alive", "ready", "unavailable"]


class StageView(ApiModel):
    id: UUID4
    strategy: Strategy
    kind: Literal["select", "review"]
    status: RunStatus
    attempt_count: int
    target_selection_id: UUID4 | None
    context_hash: str | None
    error: str | None
    receipt_available: bool


class SelectionView(ApiModel):
    id: UUID4
    strategy: Strategy
    decision: SelectionDecision
    source: Literal["model", "empty_pool", "empty_target"]


class ReviewView(ApiModel):
    id: UUID4
    strategy: Strategy
    decision: ReviewDecision
    source: Literal["model", "empty_pool", "empty_target"]


class CompanyView(ApiModel):
    symbol: str
    name: str


class QuantJobView(ApiModel):
    id: UUID4 | None
    strategy: Strategy
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    attempt_count: int | None = Field(ge=0)
    artifact_id: UUID4 | None
    analysis_hash: str | None = Field(pattern=r"^[a-f0-9]{64}$")
    error_code: str | None

    @model_validator(mode="after")
    def valid_quant_state(self) -> "QuantJobView":
        if self.status != "completed" and (self.artifact_id is not None or self.analysis_hash is not None):
            raise ValueError("未成功量化不能携带产物")
        if (self.status == "completed") != (self.artifact_id is not None and self.analysis_hash is not None):
            raise ValueError("量化状态与产物不一致")
        if (self.id is None and (self.status != "completed" or self.attempt_count is not None)) or (
            self.id is not None and self.attempt_count is None
        ):
            raise ValueError("量化尝试来源不一致")
        if self.id is not None and (
            (self.status == "pending" and self.attempt_count != 0)
            or (self.status != "pending" and self.attempt_count < 1)
        ):
            raise ValueError("量化状态与尝试计数不一致")
        if self.status in {"pending", "running", "completed"} and self.error_code is not None:
            raise ValueError("量化成功或进行状态不能携带失败错误")
        if self.status in {"failed", "cancelled"} and not self.error_code:
            raise ValueError("量化失败缺少错误")
        return self


class RunView(ApiModel):
    run_id: UUID4
    market: Literal["us"]
    target_session: date
    as_of: date
    quality: QualityReport
    snapshot_id: UUID4
    snapshot_hash: str
    report_due: AwareDatetime
    observed_at: AwareDatetime
    late: bool
    preview: bool
    status: RunStatus
    error_code: str | None
    companies: tuple[CompanyView, ...]
    selections: tuple[SelectionView, ...]
    reviews: tuple[ReviewView, ...]
    stages: tuple[StageView, ...]
    quant: tuple[QuantJobView, ...] = ()


class RunListItem(ApiModel):
    run_id: UUID4
    target_session: date
    status: RunStatus
    error_code: str | None
    create_time: AwareDatetime


class RunPage(ApiModel):
    items: tuple[RunListItem, ...]
    total: int
    page: int
    page_size: int


class RunAccepted(ApiModel):
    """202只表示已保存输入并受理，完成状态须查询对应run。"""

    run_id: UUID4
    status: RunStatus
    status_url: str
    events_url: str


class ReportView(ApiModel):
    report_id: UUID4
    revision: int
    create_time: AwareDatetime
    content_hash: str
    run: RunView


class ReportListItem(ApiModel):
    report_id: UUID4
    revision: int
    create_time: AwareDatetime
    status: RunStatus


class ReportPage(ApiModel):
    items: tuple[ReportListItem, ...]
    total: int
    page: int
    page_size: int
