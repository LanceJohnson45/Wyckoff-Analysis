"""固定运行的股票与分组事实，数值引用沿用原始量化产物。"""

from datetime import date

from pydantic import UUID4, Field, JsonValue

from stock_research.evidence import Evidence
from stock_research.schemas.api import ApiModel, CompanyView, QuantJobView, ReviewView, SelectionView, Strategy
from stock_research.schemas.parameters import ParameterValues


class HistoryBar(ApiModel):
    session_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    ref_prefix: str


class StrategyObservation(ApiModel):
    strategy: Strategy
    artifact_id: UUID4 | None
    content_hash: str | None
    parameter_hash: str
    parameters: ParameterValues
    factor_definitions: dict[str, str]
    effective_config: dict[str, JsonValue] | None
    in_pool: bool | None
    calculation_available: bool
    quant: QuantJobView | None = None
    facts: tuple[Evidence, ...]
    exclusions: tuple[Evidence, ...]
    limitations: tuple[str, ...]


class GroupObservation(ApiModel):
    name: str
    members: tuple[str, ...]
    valid_members: tuple[str, ...]
    coverage: float = Field(ge=0, le=1)
    usable: bool
    rs_median: float | None
    up_1: float | None
    up_3: float | None
    trend_health: float | None
    score: float | None
    ref_prefix: str


class StockObservation(ApiModel):
    run_id: UUID4
    report_id: UUID4 | None
    snapshot_id: UUID4
    snapshot_hash: str
    as_of: date
    company: CompanyView
    observations: tuple[StrategyObservation, ...]
    history: tuple[HistoryBar, ...]
    history_total: int
    groups: tuple[GroupObservation, ...] | None
    selections: tuple[SelectionView, ...]
    reviews: tuple[ReviewView, ...]
    limitations: tuple[str, ...]
