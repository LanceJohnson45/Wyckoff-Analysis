"""持续研究状态与可追溯事件；健康标记不覆盖有效状态。"""

from datetime import date
from typing import Literal

from pydantic import UUID4

from stock_research.schemas.api import ApiModel, Strategy

WatchState = Literal["new", "waiting", "near", "invalid"]


class WatchItem(ApiModel):
    symbol: str
    name: str
    strategy: Strategy
    state: WatchState
    target_session: date
    checked_run_id: UUID4
    state_run_id: UUID4
    state_snapshot_id: UUID4
    state_as_of: date
    state_session: date
    data_insufficient: bool
    not_reviewed: bool
    reason: str
    evidence_refs: tuple[str, ...]
    policy_version: Literal["watch-v1"] = "watch-v1"


class WatchPage(ApiModel):
    items: tuple[WatchItem, ...]
    total: int
    page: int
    page_size: int


class WatchEvent(ApiModel):
    id: UUID4
    run_id: UUID4
    target_session: date
    strategy: Strategy
    symbol: str
    kind: str
    before: WatchState | None
    after: WatchItem


class WatchEventPage(ApiModel):
    items: tuple[WatchEvent, ...]
    total: int
    page: int
    page_size: int
