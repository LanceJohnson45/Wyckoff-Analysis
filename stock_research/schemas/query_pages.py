"""MCP续页只描述一个已授权读回值，不提供文件或通用资源引用。"""

from datetime import date
from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, JsonValue

from stock_research.schemas.api import ApiModel, RunStatus


class QueryScope(ApiModel):
    report_id: UUID4 | None = None
    revision: int | None = None
    run_id: UUID4 | None = None
    snapshot_id: UUID4 | None = None
    snapshot_hash: str | None = None
    as_of: date | None = None
    target_session: date | None = None
    observed_at: AwareDatetime | None = None
    preview: bool | None = None
    late: bool | None = None
    status: RunStatus | None = None


class QueryChunk(ApiModel):
    """path保留键/下标类型；长字符串片段用字符偏移连接，其他值直接放回原位置。"""

    path: tuple[str | int, ...]
    value: JsonValue
    text_offset: int | None = None
    text_total: int | None = None


class QueryPage(ApiModel):
    """所有页共享原始内容摘要；未读完next_cursor不代表已取得完整结果。"""

    schema_version: Literal[2] = 2
    result_id: UUID4
    tool: str
    content_hash: str
    scope: QueryScope
    page: int = Field(ge=1)
    total_pages: int = Field(ge=1)
    next_cursor: str | None
    chunks: tuple[QueryChunk, ...]
