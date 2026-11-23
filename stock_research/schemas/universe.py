"""冻结整池分页与可解释的逐策略状态，不把未计算当否定。"""

from datetime import date
from typing import Literal

from pydantic import Field, field_validator, model_validator

from stock_research.schemas.api import ApiModel, CompanyView, QuantJobView, Strategy
from stock_research.schemas.report_queries import ReportScope


class UniverseQuery(ApiModel):
    page: int = Field(default=1, ge=1, le=1_000_000)
    page_size: int = Field(default=20, ge=1, le=100)
    search: str | None = Field(default=None, min_length=1, max_length=100)

    @field_validator("search")
    @classmethod
    def literal_search(cls, value: str | None) -> str | None:
        """空白不作为隐式全池搜索，NUL不能传入PostgreSQL文本。"""
        if value is not None:
            value = value.strip()
            if not value or "\x00" in value:
                raise ValueError("搜索不能为空或包含NUL")
            value.encode("utf-8")
        return value


class UniverseStrategy(ApiModel):
    strategy: Strategy
    data_eligible: bool
    calculation_available: bool
    in_pool: bool | None
    quant: QuantJobView | None = None
    selected: bool | None


class UniverseStock(ApiModel):
    company: CompanyView
    indices: tuple[Literal["sp500", "nasdaq100", "manual"], ...]
    industry: str | None
    themes: tuple[str, ...]
    history_rows: int = Field(ge=0)
    last_session: date | None
    strategies: tuple[UniverseStrategy, ...] = Field(min_length=2, max_length=2)
    ref_prefix: str

    @model_validator(mode="after")
    def consistent_stock(self) -> "UniverseStock":
        """同股不得丢策略或把无行情标成有截止日期。"""
        if {item.strategy for item in self.strategies} != {"martin", "wyckoff"}:
            raise ValueError("整池策略状态缺失或重复")
        if bool(self.history_rows) != (self.last_session is not None):
            raise ValueError("整池行情根数与日期不一致")
        return self


class UniversePage(ReportScope):
    universe_version: str
    classification_version: str
    universe_total: int = Field(ge=0)
    items: tuple[UniverseStock, ...]
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=100)
    search: str | None
    limitations: tuple[str, ...]
