"""评估worker只接收受控文件和冻结参数，不继承研究或Personal身份。"""

from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from stock_research.models import FrozenModel
from stock_research.parameters import MartinParameters
from stock_research.wyckoff import WyckoffParameters

MAX_EVALUATION_RESPONSE = 64 * 1024
MAX_EVALUATION_REQUEST = 128 * 1024
MAX_EVALUATION_REPORT = 32 * 1024 * 1024
MAX_EVALUATION_VERSION = 4 * 1024 * 1024
MAX_EVALUATION_INPUT = 512 * 1024 * 1024
MAX_EVALUATION_ROWS = 250_000


class EvaluationFiles(FrozenModel):
    """路径仅由operator配置/原任务产生，HTTP请求没有对应字段。"""

    cutoff: date
    market_path: Path
    universe_path: Path
    classification_path: Path
    artifacts_path: Path
    frozen_input_path: Path | None = None
    expected_input_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    universe_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    classification_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    martin: MartinParameters
    wyckoff: WyckoffParameters
    allow_current_universe: bool = False
    coverage_min: float = Field(default=0.95, gt=0, le=1)
    timeout_seconds: int = Field(default=3000, ge=1, le=3000)
    slippage_bps_per_side: float = Field(default=5, ge=0, le=1000)

    @model_validator(mode="after")
    def absolute_files(self) -> "EvaluationFiles":
        """子进程不从工作目录或用户home猜测输入文件。"""
        paths = (self.market_path, self.universe_path, self.classification_path, self.artifacts_path)
        if self.frozen_input_path is not None:
            paths += (self.frozen_input_path,)
        if not all(path.is_absolute() for path in paths):
            raise ValueError("评估协议路径必须绝对定位")
        return self


class EvaluationFrame(FrozenModel):
    """stdout协议不夹带自由文本异常、行情或凭据。"""

    status: Literal["started", "frozen", "section", "completed", "failed"]
    pid: int = Field(gt=0)
    source_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    completed_sections: int = Field(default=0, ge=0, le=20)
    input_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    report_hash: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    report_path: Path | None = None
    error_code: (
        Literal["evaluation_source_changed", "evaluation_input_invalid", "evaluation_calculation_failed"] | None
    ) = None

    @model_validator(mode="after")
    def coherent_frame(self) -> "EvaluationFrame":
        """成功必须携带终态身份；进度不得夹带失败或成功产物。"""
        if self.status == "completed":
            if (
                self.completed_sections != 20
                or self.input_hash is None
                or self.report_hash is None
                or self.report_path is None
                or self.error_code
            ):
                raise ValueError("评估成功协议不完整")
        elif self.report_hash is not None or self.report_path is not None:
            raise ValueError("未完成评估不能携带成功产物")
        if (self.status == "failed") != (self.error_code is not None):
            raise ValueError("评估失败协议缺少固定错误原因")
        if self.status in {"frozen", "section"} and self.input_hash is None:
            raise ValueError("评估进度缺少输入身份")
        if self.status in {"started", "frozen"} and self.completed_sections != 0:
            raise ValueError("评估启动/冻结进度无效")
        return self
