"""正式初选观测的私有资源，不混用历史量化截面或参数profile。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.schemas.api import ApiModel
from stock_research.schemas.evaluations import EvaluationStatus
from stock_research.schemas.selection_observations import SelectionOutcome, SelectionScope


class CreateSelectionObservation(ApiModel):
    report_id: UUID4
    cutoff: date


class SelectionObservationJobView(ApiModel):
    id: UUID4
    kind: Literal["formal_selection_observations"] = "formal_selection_observations"
    report_id: UUID4
    cutoff: date
    status: EvaluationStatus
    error_code: str | None
    result_id: UUID4 | None
    cancel_requested: bool = False
    attempt_count: int = Field(default=0, ge=0, le=2)
    create_time: AwareDatetime
    update_time: AwareDatetime

    @model_validator(mode="after")
    def coherent_completion(self) -> "SelectionObservationJobView":
        if self.status == "completed":
            if self.result_id is None or self.error_code is not None or self.attempt_count == 0:
                raise ValueError("完成观测缺少实际成功结果")
        elif self.result_id is not None:
            raise ValueError("未完成观测不能引用成功结果")
        return self


class SelectionObservationJobPage(ApiModel):
    items: tuple[SelectionObservationJobView, ...]
    total: int
    page: int
    page_size: int


class SelectionObservationResultView(ApiModel):
    id: UUID4
    job_id: UUID4
    kind: Literal["formal_selection_observations"] = "formal_selection_observations"
    algorithm: Literal["selection-forward-open-v1"]
    reference: Literal["first_open_after_commit"]
    scope: SelectionScope
    cutoff: date
    report_hash: str
    input_hash: str
    source_hash: str
    source_identity: str
    costs: dict[str, float]
    selection_count: int = Field(ge=0, le=2)
    observation_count: int = Field(ge=0, le=10)
    missing_strategies: tuple[Literal["martin", "wyckoff"], ...]
    manual_review: Literal["pending"] = "pending"
    limitations: tuple[str, ...]


class SelectionObservationSampleView(SelectionOutcome):
    id: UUID4
    result_id: UUID4
    reference_session: date


class SelectionObservationSamplePage(ApiModel):
    items: tuple[SelectionObservationSampleView, ...]
    total: int
    page: int
    page_size: int
