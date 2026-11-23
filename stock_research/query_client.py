"""外部入口的最小只读客户端，不读取研究数据库或模型配置。"""

from datetime import date
from typing import TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel, ValidationError

from stock_research.query_settings import QuerySettings
from stock_research.schemas.api import ApiResponse, Code, ReportView, RunView
from stock_research.schemas.observations import StockObservation

Result = TypeVar("Result", bound=BaseModel)
MAX_RESPONSE_BYTES = 2_000_000


class QueryError(ValueError):
    """公开错误不包含HTTP请求对象、凭据或驱动详情。"""


class ResearchReader:
    """路径只由类型化业务方法生成，禁止任意URL、方法或写操作。"""

    def __init__(self, settings: QuerySettings, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings, self.transport = settings, transport

    async def _read(self, path: str, model: type[Result], params: dict[str, str]) -> Result:
        try:
            async with httpx.AsyncClient(
                base_url=self.settings.endpoint,
                headers={"Authorization": "Bearer " + self.settings.token.get_secret_value()},
                timeout=20,
                follow_redirects=False,
                trust_env=False,
                transport=self.transport,
            ) as client:
                async with client.stream("GET", path, params=params) as response:
                    parts, size = [], 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_RESPONSE_BYTES:
                            raise QueryError("研究响应超过上限，请缩小查询范围")
                        parts.append(chunk)
                    raw = b"".join(parts)
                    if response.status_code != 200:
                        raise QueryError("研究查询未成功，请核对只读身份、运行标识及服务状态")
            packet = ApiResponse[model].model_validate_json(raw)
            if packet.code != Code.OK or packet.data is None:
                raise QueryError("研究查询未返回有效事实，请核对服务状态")
            return packet.data
        except (httpx.HTTPError, ValidationError):
            raise QueryError("研究连接或响应无效，请核对本机服务与接口版本") from None

    async def latest_report(self, target_session: date | None = None) -> ReportView:
        """最新是最近创建运行的固定修订，可明确包含partial或迟到。"""
        return await self._read(
            "/v1/reports/latest", ReportView, {"target_session": target_session.isoformat()} if target_session else {}
        )

    async def run_status(self, run_id: UUID) -> RunView:
        """业务UUID限定运行，不接收URL或文件路径。"""
        return await self._read(f"/v1/research-runs/{run_id}", RunView, {})

    async def stock(
        self, symbol: str, run_id: UUID, limit: int = 60, report_id: UUID | None = None
    ) -> StockObservation:
        """只查给定运行的股票事实，不缺数补零或联网刷新。"""
        import re

        if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,31}", symbol) or isinstance(limit, bool) or not 1 <= limit <= 320:
            raise QueryError("股票代码或历史范围无效")
        return await self._read(
            f"/v1/stocks/{symbol}/observations",
            StockObservation,
            {"run_id": str(run_id), "limit": str(limit), **({"report_id": str(report_id)} if report_id else {})},
        )
