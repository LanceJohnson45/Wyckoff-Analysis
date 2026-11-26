"""正式初选worker只接收已确认范围及固定操作路径。"""

from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stock_research.models import FrozenModel
from stock_research.schemas.selection_observations import SelectionScope


class SelectionObservationFiles(FrozenModel):
    scope: SelectionScope
    cutoff: date
    market_path: Path
    artifacts_path: Path
    frozen_input_path: Path
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_input_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    slippage_bps_per_side: float = Field(default=5, ge=0, le=1000)

    @model_validator(mode="after")
    def absolute_files(self) -> "SelectionObservationFiles":
        if not all(path.is_absolute() for path in (self.market_path, self.artifacts_path, self.frozen_input_path)):
            raise ValueError("观测协议路径必须绝对定位")
        return self


class SelectionObservationFrame(FrozenModel):
    status: Literal["started", "frozen", "completed", "failed"]
    pid: int = Field(gt=0)
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    input_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    report_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    report_path: Path | None = None
    error_code: (
        Literal["evaluation_source_changed", "evaluation_input_invalid", "evaluation_calculation_failed"] | None
    ) = None

    @model_validator(mode="after")
    def coherent_frame(self) -> "SelectionObservationFrame":
        if self.status == "completed":
            if self.input_hash is None or self.report_hash is None or self.report_path is None or self.error_code:
                raise ValueError("观测成功协议不完整")
        elif self.report_hash is not None or self.report_path is not None:
            raise ValueError("未完成观测不能携带成功产物")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("观测失败协议缺少固定原因")
        if self.status == "frozen" and self.input_hash is None:
            raise ValueError("观测冻结缺少输入身份")
        if self.status == "started" and self.input_hash is not None:
            raise ValueError("观测启动不能提前确认输入")
        return self
