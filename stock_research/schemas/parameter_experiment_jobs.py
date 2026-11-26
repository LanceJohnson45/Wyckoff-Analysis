"""首轮参数实验的私有作业契约；不发布生产参数，不承接任意字段。"""

from datetime import date
from typing import Annotated, Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.parameter_experiments import ExperimentField
from stock_research.schemas.api import ApiModel
from stock_research.schemas.evaluations import EvaluationStatus

FiniteValue = Annotated[float, Field(allow_inf_nan=False)]
ExperimentValues = tuple[FiniteValue, FiniteValue, FiniteValue]


class CreateParameterExperiment(ApiModel):
    profile: str = Field(min_length=1, max_length=256, pattern=r"^[^\x00]+$")
    cutoff: date
    field: ExperimentField
    values: ExperimentValues

    @model_validator(mode="after")
    def distinct_values(self) -> "CreateParameterExperiment":
        if len(set(self.values)) != 3:
            raise ValueError("三个变体值必须互异")
        return self


class ParameterExperimentJobView(ApiModel):
    id: UUID4
    kind: Literal["parameter_experiments"] = "parameter_experiments"
    profile: str
    cutoff: date
    field: ExperimentField
    values: ExperimentValues
    status: EvaluationStatus
    error_code: str | None
    result_id: UUID4 | None
    cancel_requested: bool = False
    attempt_count: int = Field(default=0, ge=0, le=2)
    create_time: AwareDatetime
    update_time: AwareDatetime

    @model_validator(mode="after")
    def coherent_completion(self) -> "ParameterExperimentJobView":
        if self.status == "completed":
            if self.result_id is None or self.error_code is not None or self.attempt_count == 0:
                raise ValueError("完成实验缺少实际成功结果")
        elif self.result_id is not None:
            raise ValueError("未完成实验不能引用成功结果")
        return self


class ParameterExperimentJobPage(ApiModel):
    items: tuple[ParameterExperimentJobView, ...]
    total: int
    page: int
    page_size: int


class ParameterExperimentVariant(ApiModel):
    report_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    input_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    parameter_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    value: float
    candidate_count: int = Field(ge=0)
    usable_sections: dict[str, int]
    manual_review: Literal["pending"]
    limitations: tuple[str, ...]


class ParameterExperimentResultView(ApiModel):
    id: UUID4
    job_id: UUID4
    kind: Literal["parameter_experiments"] = "parameter_experiments"
    profile: str
    cutoff: date
    field: ExperimentField
    values: ExperimentValues
    input_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    variants: tuple[ParameterExperimentVariant, ...]
    manual_review: Literal["pending"] = "pending"
    limitations: tuple[str, ...]
