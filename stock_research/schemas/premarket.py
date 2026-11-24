"""盘前固定任务的输入、冻结期限与只读回执。"""

from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.calendar import exchange_calendar
from stock_research.schemas.api import ApiModel, ReportView, RunView
from stock_research.schemas.preparations import PreparationView

NEW_YORK = ZoneInfo("America/New_York")


class ScheduleContext(ApiModel):
    """唯一股票计划及可核对的原事件身份，不接受命令、路径或凭据。"""

    schedule_id: Literal["stock-research-premarket"]
    event_id: str = Field(min_length=1, max_length=160)
    scheduled_for: AwareDatetime
    workspace_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
    trigger_kind: Literal["scheduled", "manual"]

    @model_validator(mode="after")
    def coherent_event(self) -> "ScheduleContext":
        stamp = self.scheduled_for.astimezone(UTC).isoformat().replace("+00:00", "Z")
        prefix = "manual-" if self.trigger_kind == "manual" else ""
        if self.event_id != f"{self.schedule_id}:{prefix}{stamp}":
            raise ValueError("原计划时刻与事件身份不一致")
        local = self.scheduled_for.astimezone(NEW_YORK)
        if self.trigger_kind == "scheduled" and local.timetz().replace(tzinfo=None) != time(8):
            raise ValueError("自动盘前事件必须为纽约08:00的原计划时刻")
        self.scheduled_for = self.scheduled_for.astimezone(UTC)
        if not 1990 <= self.target_session.year <= 2040:
            raise ValueError("盘前目标日期超出已安装日历范围")
        if self.target_session <= exchange_calendar().first_session.date():
            raise ValueError("盘前日期的前一交易日超出日历范围")
        return self

    @property
    def target_session(self) -> date:
        return self.scheduled_for.astimezone(NEW_YORK).date()

    @property
    def execution_deadline(self) -> datetime:
        if self.trigger_kind == "manual":
            return self.scheduled_for + timedelta(seconds=3000)
        return datetime.combine(self.target_session, time(8, 50), NEW_YORK)


class PremarketInput(ApiModel):
    schedule: ScheduleContext


class PremarketPlan(ApiModel):
    mode: Literal["plan"] = "plan"
    research_started: Literal[False] = False
    schedule: ScheduleContext
    target_session: date
    observed_at: AwareDatetime
    as_of: date | None
    report_due: AwareDatetime | None
    recovery_until: AwareDatetime | None
    execution_deadline: AwareDatetime | None
    budget_seconds: int = Field(ge=0, le=3000)
    late: bool
    status: Literal["skipped_non_session", "before_start", "within_window", "expired", "late_manual"]


class PremarketBinding(ApiModel):
    job_id: UUID4
    schedule: ScheduleContext
    execution_deadline: AwareDatetime

    @model_validator(mode="after")
    def original_deadline(self) -> "PremarketBinding":
        if self.execution_deadline != self.schedule.execution_deadline:
            raise ValueError("盘前期限必须等于原事件的固定期限")
        return self


class PremarketReceipt(ApiModel):
    job_id: UUID4
    plan: PremarketPlan
    status: Literal[
        "skipped",
        "unconfirmed",
        "failed_preparation",
        "expired",
        "pending",
        "running",
        "completed",
        "partial",
        "failed",
        "cancelled",
    ]
    preparation: PreparationView | None
    run: RunView | None
    report: ReportView | None

    @model_validator(mode="after")
    def coherent_references(self) -> "PremarketReceipt":
        if self.run is None and self.status in {"pending", "running", "completed", "partial", "failed"}:
            raise ValueError("盘前运行状态缺少实际运行")
        if self.run and self.status not in {self.run.status, "cancelled"}:
            raise ValueError("盘前状态与实际运行不一致")
        if self.preparation and (
            self.preparation.request_id != self.job_id or self.preparation.target_session != self.plan.target_session
        ):
            raise ValueError("盘前准备回执不属于原任务")
        if self.run and (self.run.target_session != self.plan.target_session or self.run.as_of != self.plan.as_of):
            raise ValueError("盘前运行不属于原计划")
        if self.report and (
            not self.run
            or self.report.run.run_id != self.run.run_id
            or self.report.run.snapshot_hash != self.run.snapshot_hash
        ):
            raise ValueError("盘前固定报告不属于原运行")
        if (
            self.preparation
            and self.preparation.run_id
            and (not self.run or self.preparation.run_id != self.run.run_id)
        ):
            raise ValueError("盘前准备与运行引用不一致")
        return self
