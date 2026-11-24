"""行情更新的内部管道协议与可公开的逐步回执。"""

from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.calendar import latest_complete_session, sessions_ending
from stock_research.config import MarketSourceSettings
from stock_research.schemas.api import ApiModel

MAX_REFRESH_FRAME = 16384
RefreshErrorCode = Literal[
    "refresh_configuration_invalid",
    "refresh_source_changed",
    "refresh_sync_failed",
    "refresh_benchmark_failed",
    "refresh_protocol_invalid",
    "refresh_process_failed",
    "refresh_timeout",
    "refresh_cancelled",
]


class SyncSummary(ApiModel):
    market: Literal["us"]
    rows: int = Field(ge=0)
    batches: int = Field(ge=0)
    rejected: int = Field(ge=0)
    issues: int = Field(ge=0)
    checkpoint: AwareDatetime
    scope: Literal["research:us:all:benchmarks-v1"]


class RefreshReceipt(ApiModel):
    id: UUID4
    as_of: date
    source_hash: str
    status: Literal["unconfirmed", "completed", "failed", "cancelled"]
    started_at: AwareDatetime
    finished_at: AwareDatetime | None
    sync: SyncSummary | None
    benchmark_hash: str | None
    error_code: RefreshErrorCode | None

    @model_validator(mode="after")
    def coherent_status(self) -> "RefreshReceipt":
        if (self.status == "unconfirmed") != (self.finished_at is None):
            raise ValueError("行情更新终态缺少确认时间")
        if (self.status in {"failed", "cancelled"}) != bool(self.error_code):
            raise ValueError("行情更新终态与错误不一致")
        if self.status == "completed" and self.sync is None:
            raise ValueError("行情更新完成缺少同步回执")
        return self


class RefreshRequest(ApiModel):
    id: UUID4
    source_hash: str
    as_of: date
    history_rows: int = Field(ge=320)
    local_path: Path
    benchmark_directory: Path
    fetch_benchmarks: bool
    deadline: AwareDatetime
    source: MarketSourceSettings

    @model_validator(mode="after")
    def fixed_paths(self) -> "RefreshRequest":
        if not self.local_path.is_absolute() or not self.benchmark_directory.is_absolute():
            raise ValueError("行情更新路径必须是operator固定的绝对路径")
        if self.as_of > latest_complete_session():
            raise ValueError("行情更新截止日尚未完成收盘")
        sessions_ending(self.as_of, self.history_rows)
        self.source.database_url()
        return self


class RefreshEvent(ApiModel):
    kind: Literal["started", "synced", "finished"]
    id: UUID4
    source_hash: str
    pid: int = Field(gt=0)
    sync: SyncSummary | None = None
    benchmark_path: Path | None = None
    benchmark_hash: str | None = None
    error_code: RefreshErrorCode | None = None

    @model_validator(mode="after")
    def phase_fields(self) -> "RefreshEvent":
        extra = self.benchmark_path is not None or self.benchmark_hash is not None or self.error_code is not None
        if self.kind == "started" and (self.sync is not None or extra):
            raise ValueError("启动帧不能携带后续步骤结果")
        if self.kind == "synced" and (self.sync is None or extra):
            raise ValueError("同步帧缺少完整结果")
        if self.kind == "finished" and (
            self.sync is not None
            or bool(self.benchmark_path) != bool(self.benchmark_hash)
            or (self.error_code is not None and self.benchmark_path is not None)
        ):
            raise ValueError("终态帧结果不一致")
        return self
