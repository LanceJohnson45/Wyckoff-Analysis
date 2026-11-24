"""固定正式初选及其前向开盘观测的不可变文件契约。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.evaluation_outcomes import OBSERVATION_HORIZONS, OutcomeWindow
from stock_research.identity import digest
from stock_research.models import Bar, FrozenModel, QualityIssue
from stock_research.schemas.api import RunStatus, Strategy


class SelectionBinding(FrozenModel):
    selection_id: UUID4
    strategy: Strategy
    selection_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    context_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    committed_at: AwareDatetime
    reference_session: date
    source: Literal["model", "empty_pool"]
    symbols: tuple[str, ...] = Field(max_length=5)

    @model_validator(mode="after")
    def unique_symbols(self) -> "SelectionBinding":
        if len(set(self.symbols)) != len(self.symbols) or self.symbols and self.source != "model":
            raise ValueError("正式初选来源或股票不一致")
        return self


class SelectionScope(FrozenModel):
    workspace: str
    report_id: UUID4
    report_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    report_revision: int = Field(ge=1)
    report_status: RunStatus
    run_id: UUID4
    snapshot_id: UUID4
    snapshot_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    target_session: date
    selections: tuple[SelectionBinding, ...] = Field(max_length=2)

    @model_validator(mode="after")
    def unique_selections(self) -> "SelectionScope":
        if len({row.strategy for row in self.selections}) != len(self.selections):
            raise ValueError("同一报告有重复策略初选")
        return self


class SelectionObservationInput(FrozenModel):
    scope: SelectionScope
    cutoff: date
    source_hash: str
    source_identity: str
    costs: dict[str, float]
    bars: tuple[Bar, ...]
    issues: tuple[QualityIssue, ...]


class SelectionOutcome(FrozenModel):
    selection_id: UUID4
    strategy: Strategy
    symbol: str
    windows: tuple[OutcomeWindow, ...]

    @model_validator(mode="after")
    def complete_windows(self) -> "SelectionOutcome":
        if tuple(row.horizon for row in self.windows) != OBSERVATION_HORIZONS:
            raise ValueError("正式初选观测窗口不完整")
        return self


class SelectionObservationReport(FrozenModel):
    kind: Literal["formal_selection_observations"] = "formal_selection_observations"
    algorithm: Literal["selection-forward-open-v1"] = "selection-forward-open-v1"
    reference: Literal["first_open_after_commit"] = "first_open_after_commit"
    scope: SelectionScope
    cutoff: date
    source_hash: str
    input_hash: str
    source_identity: str
    costs: dict[str, float]
    samples: tuple[SelectionOutcome, ...]
    limitations: tuple[str, ...]
    content_hash: str

    @model_validator(mode="after")
    def validate_content(self) -> "SelectionObservationReport":
        if self.content_hash != digest(self.model_dump(mode="json", exclude={"content_hash"})):
            raise ValueError("正式初选观测摘要不一致")
        expected = {(row.selection_id, row.strategy, symbol) for row in self.scope.selections for symbol in row.symbols}
        actual = [(row.selection_id, row.strategy, row.symbol) for row in self.samples]
        if len(actual) != len(set(actual)) or set(actual) != expected:
            raise ValueError("正式初选观测分母与固定名单不一致")
        return self
