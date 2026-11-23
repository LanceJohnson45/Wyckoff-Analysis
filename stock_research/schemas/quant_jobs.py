"""量化尝试的公开分页，不返回临时文件或执行身份。"""

from pydantic import UUID4, AwareDatetime, Field

from stock_research.schemas.api import ApiModel, QuantJobView


class QuantAttemptView(QuantJobView):
    id: UUID4
    job_id: UUID4
    attempt_count: int = Field(ge=1)
    create_time: AwareDatetime
    update_time: AwareDatetime


class QuantAttemptPage(ApiModel):
    run_id: UUID4
    recorded: bool
    items: tuple[QuantAttemptView, ...]
    total: int = Field(ge=0)
    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=20)
