"""未建立研究运行时也可独立查询的输入准备回执。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, model_validator

from stock_research.models import QualityReport
from stock_research.schemas.api import ApiModel

PreparationStatus = Literal["unconfirmed", "ready", "failed", "cancelled"]
PreparationPhase = Literal["parameters", "snapshot", "analysis", "freeze"]
PreparationError = Literal["quality_failed", "input_unavailable", "source_changed", "cancelled", "incomplete"]


class PreparationState(ApiModel):
    market: Literal["us"] = "us"
    target_session: date
    as_of: date
    origin: Literal["api", "cli"]
    request_id: UUID4 | None
    status: PreparationStatus
    phase: PreparationPhase
    quality: QualityReport | None
    error_code: PreparationError | None
    run_id: UUID4 | None
    source_hash: str

    @model_validator(mode="after")
    def consistent_result(self) -> "PreparationState":
        """未确认不冒充成功，ready必须指向固定运行且没有错误。"""
        if (self.origin == "api") != bool(self.request_id):
            raise ValueError("准备来源与请求引用不一致")
        if (self.status == "ready") != bool(self.run_id):
            raise ValueError("准备状态与运行引用不一致")
        if (self.status in {"failed", "cancelled"}) != bool(self.error_code):
            raise ValueError("准备状态与错误不一致")
        if self.status == "ready" and (self.phase != "freeze" or not self.quality or not self.quality.passed):
            raise ValueError("准备完成缺少已通过质量检查的冻结输入")
        return self


class PreparationView(PreparationState):
    id: UUID4
    create_time: AwareDatetime
    update_time: AwareDatetime


class PreparationPage(ApiModel):
    items: tuple[PreparationView, ...]
    total: int
    page: int
    page_size: int
