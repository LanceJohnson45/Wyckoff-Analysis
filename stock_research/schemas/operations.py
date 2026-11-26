"""操作状态的小型公开投影，不含通知正文、目标身份或内部路径。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, model_validator

from stock_research.schemas.api import ApiModel

DeliveryState = Literal[
    "not_recorded", "preparing", "ready", "sending", "retry", "unknown", "failed", "silent", "sent", "unconfirmed"
]


class DeliveryPhase(ApiModel):
    state: DeliveryState
    ledger_update_time: AwareDatetime | None
    has_native_receipt: bool

    @model_validator(mode="after")
    def consistent_receipt(self) -> "DeliveryPhase":
        if self.state == "not_recorded":
            if self.ledger_update_time is not None or self.has_native_receipt:
                raise ValueError("无记录不能携带回执或更新时间")
        elif self.ledger_update_time is None or self.state == "sent" and not self.has_native_receipt:
            raise ValueError("通知状态缺少账本时间或原生回执")
        return self


class LatestDelivery(ApiModel):
    target_session: date
    phase: Literal["initial", "recovery"]
    ledger_update_time: AwareDatetime


class DeliveryStatus(ApiModel):
    target_session: date
    checked_at: AwareDatetime
    initial: DeliveryPhase
    recovery: DeliveryPhase
    latest_sent: LatestDelivery | None


class CompletedResearch(ApiModel):
    run_id: UUID4
    target_session: date
    update_time: AwareDatetime


class OperationsView(ApiModel):
    target_session: date
    checked_at: AwareDatetime
    latest_completed: CompletedResearch | None
    delivery: DeliveryStatus | None

    @model_validator(mode="after")
    def same_target(self) -> "OperationsView":
        if self.delivery and self.delivery.target_session != self.target_session:
            raise ValueError("通知回执与目标日不一致")
        return self
