"""工作台评估仅接受研究意图，不接受路径、SQL或模型连接。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.evaluation_outcomes import OutcomeWindow
from stock_research.evaluation_sampling import EvaluationSection
from stock_research.schemas.api import ApiModel, Strategy

EvaluationStatus = Literal["pending", "running", "completed", "failed", "cancelled"]


class CreateEvaluation(ApiModel):
    """基线量化池回放与正式初选观测分别演进，不混用标签。"""

    profile: str = Field(min_length=1, max_length=256, pattern=r"^[^\x00]+$")
    cutoff: date
    allow_current_universe: bool = False


class EvaluationJobView(ApiModel):
    """公开状态不含worker私有路径或任何连接资料。"""

    id: UUID4
    kind: Literal["historical_quant_pool"] = "historical_quant_pool"
    profile: str
    cutoff: date
    allow_current_universe: bool
    status: EvaluationStatus
    completed_sections: int = Field(ge=0, le=20)
    error_code: str | None
    result_id: UUID4 | None
    cancel_requested: bool = False
    attempt_count: int = Field(default=0, ge=0, le=2)
    create_time: AwareDatetime
    update_time: AwareDatetime

    @model_validator(mode="after")
    def coherent_completion(self) -> "EvaluationJobView":
        if self.status == "completed":
            if self.result_id is None or self.completed_sections != 20 or self.error_code is not None:
                raise ValueError("完成评估缺少成功结果或进度不一致")
        elif self.result_id is not None:
            raise ValueError("未完成评估不能引用成功结果")
        return self


class EvaluationJobPage(ApiModel):
    items: tuple[EvaluationJobView, ...]
    total: int
    page: int
    page_size: int


class EvaluationSectionSummary(ApiModel):
    section: EvaluationSection
    strategy: Strategy
    snapshot_hash: str
    parameter_hash: str
    analysis_hash: str
    universe_count: int
    eligible_count: int
    observation_count: int
    exclusions: dict[str, int]
    pool: tuple[str, ...]


class EvaluationResultView(ApiModel):
    id: UUID4
    job_id: UUID4
    kind: Literal["historical_quant_pool"]
    report_hash: str
    input_hash: str
    source_hash: str
    parameter_hashes: dict[str, str]
    cutoff: date
    allow_current_universe: bool
    candidate_count: int
    usable_sections: dict[str, int]
    sections: tuple[EvaluationSectionSummary, ...]
    model_selection: None
    manual_review: Literal["pending"]
    limitations: tuple[str, ...]


class EvaluationSample(ApiModel):
    id: UUID4
    result_id: UUID4
    as_of: date
    strategy: Strategy
    symbol: str
    partition: Literal["research", "validation", "sealed"]
    purged: bool
    status: Literal["completed", "pending", "missing_data", "sealed"]
    windows: tuple[OutcomeWindow, ...] | None

    @model_validator(mode="after")
    def coherent_windows(self) -> "EvaluationSample":
        if self.partition == "sealed":
            if self.status != "sealed" or self.windows is not None:
                raise ValueError("封存样本不能公开表现")
        elif (
            self.status == "sealed"
            or self.windows is None
            or tuple(row.horizon for row in self.windows) != (1, 3, 5, 10, 20)
        ):
            raise ValueError("观测窗口不完整或封存状态不一致")
        return self


class EvaluationSamplePage(ApiModel):
    items: tuple[EvaluationSample, ...]
    total: int
    page: int
    page_size: int
