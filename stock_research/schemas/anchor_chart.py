"""固定报告的显示用锚点目录与单条累计均价，不是新增策略结果。"""

from datetime import date
from typing import Literal

from pydantic import Field, FiniteFloat, model_validator

from stock_research.schemas.api import ApiModel, CompanyView
from stock_research.schemas.report_queries import ReportScope


class AnchorDefinition(ApiModel):
    index: int = Field(ge=0)
    kind: Literal["swing_low", "swing_high", "breakout"]
    date: date
    confirmed_date: date
    daily_estimate: FiniteFloat | None
    hard_filter_enabled: Literal[False]
    ref_prefix: str


class AnchorPrice(ApiModel):
    session_date: date
    value: FiniteFloat | None


class AnchorChart(ReportScope):
    company: CompanyView
    calculation_available: bool
    anchors: tuple[AnchorDefinition, ...]
    selected_index: int | None
    points: tuple[AnchorPrice, ...]
    limitations: tuple[str, ...]

    @model_validator(mode="after")
    def validate_projection(self) -> "AnchorChart":
        """读取端也拒绝未来、错引用及脱离所选锚点的曲线。"""
        if tuple(row.index for row in self.anchors) != tuple(range(len(self.anchors))):
            raise ValueError("锚点索引顺序无效")
        if any(
            row.date > row.confirmed_date
            or row.confirmed_date > self.as_of
            or row.ref_prefix != f"martin/{self.company.symbol}/anchors/{row.index}"
            for row in self.anchors
        ):
            raise ValueError("锚点确认日或引用无效")
        if (not self.calculation_available and self.anchors) or (
            self.selected_index is None and (self.anchors or self.points)
        ):
            raise ValueError("锚点目录与计算状态不一致")
        if self.selected_index is not None:
            if not 0 <= self.selected_index < len(self.anchors):
                raise ValueError("所选锚点不存在")
            confirmed = self.anchors[self.selected_index].confirmed_date
            dates = [row.session_date for row in self.points]
            if len(dates) > 320 or any(
                day < confirmed or day > self.as_of or (index > 0 and day <= dates[index - 1])
                for index, day in enumerate(dates)
            ):
                raise ValueError("曲线超出确认后行情或顺序无效")
        return self
