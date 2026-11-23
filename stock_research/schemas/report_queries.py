"""固定报告级只读查询，不以当前参数或行情替代历史输入。"""

from datetime import date
from typing import Annotated

from pydantic import UUID4, Field, JsonValue, StringConstraints, model_validator

from stock_research.evidence import Evidence
from stock_research.models import QualityReport
from stock_research.schemas.api import ApiModel, CompanyView, QuantJobView, Strategy
from stock_research.schemas.observations import GroupObservation, HistoryBar, StockObservation
from stock_research.schemas.parameters import ParameterValues

Symbol = Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")]


class ReportScope(ApiModel):
    run_id: UUID4
    report_id: UUID4
    snapshot_id: UUID4
    snapshot_hash: str
    as_of: date
    target_session: date


class QuantitativeScope(ApiModel):
    strategy: Strategy
    artifact_id: UUID4 | None
    content_hash: str | None
    parameter_hash: str
    calculated_count: int | None
    exclusion_record_count: int | None
    pool: tuple[CompanyView, ...] | None
    quant: QuantJobView | None = None


class BenchmarkHistory(ApiModel):
    symbol: str
    history: tuple[HistoryBar, ...]
    history_total: int


class MarketOverview(ReportScope):
    quality: QualityReport
    market_facts: tuple[Evidence, ...]
    benchmarks: tuple[BenchmarkHistory, ...]
    strategies: tuple[QuantitativeScope, ...]
    limitations: tuple[str, ...]


class GroupPage(ReportScope):
    items: tuple[GroupObservation, ...] | None
    total: int | None
    page: int
    page_size: int
    limitations: tuple[str, ...]
    quant: QuantJobView | None = None


class ComparisonInput(ApiModel):
    symbols: tuple[Symbol, ...] = Field(min_length=2, max_length=8)

    @model_validator(mode="after")
    def distinct_symbols(self) -> "ComparisonInput":
        """重复代码不制造多个比较对象，也不隐式去重改变用户请求。"""
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("比较代码不能重复")
        return self


class StockComparison(ReportScope):
    items: tuple[StockObservation, ...]


class FrozenRules(ApiModel):
    strategy: Strategy
    artifact_id: UUID4 | None
    content_hash: str | None
    parameter_hash: str
    parameters: ParameterValues
    factor_definitions: dict[str, str]
    effective_config: dict[str, JsonValue] | None
    limitations: tuple[str, ...]
    quant: QuantJobView | None = None


class StrategyRules(ReportScope):
    source_hash: str
    prompt_hash: str
    strategies: tuple[FrozenRules, ...]
