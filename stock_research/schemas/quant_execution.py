"""父子量化进程的固定协议，不接受模型/数据库或网络连接配置。"""

from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stock_research.models import FrozenModel
from stock_research.parameters import MartinParameters
from stock_research.wyckoff import WyckoffParameters

MAX_QUANT_REQUEST = 128 * 1024
MAX_QUANT_RESPONSE = 64 * 1024
MAX_QUANT_SNAPSHOT = 512 * 1024 * 1024
MAX_QUANT_REPORT = 64 * 1024 * 1024
QUANT_TIMEOUT_SECONDS = 120


class QuantRequest(FrozenModel):
    strategy: Literal["martin", "wyckoff"]
    snapshot_path: Path
    snapshot_id: str
    snapshot_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_path: Path
    parameters: MartinParameters | WyckoffParameters

    @model_validator(mode="after")
    def valid_input(self) -> "QuantRequest":
        """worker不能把策略参数混用，也不从临时cwd猜相对文件。"""
        if not self.snapshot_path.is_absolute() or not self.output_path.is_absolute():
            raise ValueError("量化协议路径必须绝对定位")
        model = MartinParameters if self.strategy == "martin" else WyckoffParameters
        if not isinstance(self.parameters, model):
            raise ValueError("量化参数与策略不一致")
        return self


class QuantStarted(FrozenModel):
    status: Literal["started"]
    pid: int = Field(gt=0)
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    strategy: Literal["martin", "wyckoff"]


class QuantFinished(FrozenModel):
    status: Literal["completed", "failed"]
    content_hash: str | None
    error_code: Literal["quant_calculation_failed", "quant_source_changed", "quant_input_invalid"] | None

    @model_validator(mode="after")
    def valid_outcome(self) -> "QuantFinished":
        """失败不能同时携带成功摘要，成功必须有完整产物身份。"""
        if (self.status == "completed") != (self.content_hash is not None and self.error_code is None):
            raise ValueError("量化结果状态不一致")
        if self.status == "failed" and (self.content_hash is not None or self.error_code is None):
            raise ValueError("量化失败缺少明确错误")
        return self
