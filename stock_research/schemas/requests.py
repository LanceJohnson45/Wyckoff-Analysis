"""研究动作请求只接受业务标识，不接受路径、连接或模型配置。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, model_validator

from stock_research.schemas.api import ApiModel


class CreateRun(ApiModel):
    market: Literal["us"] = "us"
    target_session: date
    parent_run_id: UUID4 | None = None
    force: bool = False
    preview: Literal[True] = True

    @model_validator(mode="after")
    def require_parent(self) -> "CreateRun":
        """重分析显式关联父版本，不能隐式覆盖已有初选。"""
        if self.force != bool(self.parent_run_id):
            raise ValueError("force与parent_run_id须同时提供")
        return self
