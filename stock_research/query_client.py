"""外部入口的最小只读客户端，不读取研究数据库或模型配置。"""

from datetime import date
from typing import Literal, TypeVar
from uuid import UUID

import httpx
from pydantic import BaseModel, TypeAdapter, ValidationError

from stock_research.query_settings import QuerySettings
from stock_research.schemas.anchor_chart import AnchorChart
from stock_research.schemas.api import ApiResponse, Code, ReportView, RunView
from stock_research.schemas.observations import StockObservation
from stock_research.schemas.quant_jobs import QuantAttemptPage
from stock_research.schemas.report_queries import (
    ComparisonInput,
    GroupPage,
    MarketOverview,
    ReportScope,
    StockComparison,
    StrategyRules,
    Symbol,
)
from stock_research.schemas.universe import UniversePage, UniverseQuery

Result = TypeVar("Result", bound=BaseModel)
ScopedResult = TypeVar("ScopedResult", bound=ReportScope)
MAX_RESPONSE_BYTES = 2_000_000


class QueryError(ValueError):
    """公开错误不包含HTTP请求对象、凭据或驱动详情。"""


class ResearchReader:
    """路径只由类型化业务方法生成，禁止任意URL、方法或写操作。"""

    def __init__(self, settings: QuerySettings, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings, self.transport = settings, transport

    async def _read(self, path: str, model: type[Result], params: dict[str, str | list[str]]) -> Result:
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

    async def report(self, report_id: UUID) -> ReportView:
        """重读明确修订，避免用latest替代讨论中的固定报告。"""
        value = await self._read(f"/v1/reports/{report_id}", ReportView, {})
        if value.report_id != report_id:
            raise QueryError("响应与指定固定报告不一致")
        return value

    async def _scoped(
        self, report_id: UUID, resource: str, model: type[ScopedResult], params: dict[str, str | list[str]]
    ) -> ScopedResult:
        result = await self._read(f"/v1/reports/{report_id}/{resource}", model, params)
        if result.report_id != report_id:
            raise QueryError("研究响应与指定报告不一致")
        return result

    async def market_overview(self, report_id: UUID, limit: int = 60) -> MarketOverview:
        """三基准和市场事实均属于指定修订，不联网或重算。"""
        if isinstance(limit, bool) or not 1 <= limit <= 320:
            raise QueryError("市场历史范围无效")
        return await self._scoped(report_id, "market", MarketOverview, {"limit": str(limit)})

    async def universe(self, report_id: UUID, query: UniverseQuery | None = None) -> UniversePage:
        """整池去重总数与搜索总数分别返回，查询限于固定报告。"""
        query = query or UniverseQuery()
        return await self._scoped(
            report_id,
            "universe",
            UniversePage,
            {key: str(value) for key, value in query.model_dump(exclude_none=True).items()},
        )

    async def group_strength(
        self,
        report_id: UUID,
        page: int = 1,
        page_size: int = 20,
        kind: Literal["industry", "theme"] | None = None,
        name: str | None = None,
    ) -> GroupPage:
        """按原分组得分降序及组名排序，NULL最后，分页不改变有效分母。"""
        if (
            isinstance(page, bool)
            or isinstance(page_size, bool)
            or page < 1
            or not 1 <= page_size <= 100
            or kind not in {None, "industry", "theme"}
            or (name is not None and not 1 <= len(name) <= 512)
        ):
            raise QueryError("分组分页或过滤参数无效")
        return await self._scoped(
            report_id,
            "groups",
            GroupPage,
            {
                "page": str(page),
                "page_size": str(page_size),
                **({"kind": kind} if kind else {}),
                **({"name": name} if name is not None else {}),
            },
        )

    async def compare_stocks(self, report_id: UUID, symbols: tuple[str, ...], limit: int = 20) -> StockComparison:
        """比较2～8只同报告股票，保持请求顺序和原事实，不生成合并得分。"""
        try:
            requested = ComparisonInput(symbols=symbols)
        except ValueError:
            raise QueryError("比较须包含2～8个不同的大写规范代码") from None
        if isinstance(limit, bool) or not 1 <= limit <= 60:
            raise QueryError("比较历史范围无效")
        return await self._scoped(
            report_id, "compare", StockComparison, {"symbols": list(requested.symbols), "limit": str(limit)}
        )

    async def strategy_rules(self, report_id: UUID) -> StrategyRules:
        """读取冻结规则的来源、参数与有效配置，不读取当前发布。"""
        return await self._scoped(report_id, "rules", StrategyRules, {})

    async def anchor_chart(
        self, report_id: UUID, symbol: str, limit: int = 320, index: int | None = None
    ) -> AnchorChart:
        """仅取一个原锚点曲线，不提供任意路径或结构重算入口。"""
        try:
            TypeAdapter(Symbol).validate_python(symbol)
        except ValueError:
            raise QueryError("锚点股票代码无效") from None
        if (
            isinstance(limit, bool)
            or not 1 <= limit <= 320
            or (index is not None and (isinstance(index, bool) or index < 0))
        ):
            raise QueryError("锚点范围或索引无效")
        value = await self._scoped(
            report_id,
            f"anchors/{symbol}",
            AnchorChart,
            {"limit": str(limit), **({"index": str(index)} if index is not None else {})},
        )
        if value.company.symbol != symbol or (index is not None and value.selected_index != index):
            raise QueryError("锚点响应与指定股票或索引不一致")
        return value

    async def quant_attempts(self, run_id: UUID, page: int = 1, page_size: int = 10) -> QuantAttemptPage:
        """本运行当前尝试历史独立分页，不改固定报告当时状态。"""
        if (
            isinstance(page, bool)
            or isinstance(page_size, bool)
            or not 1 <= page <= 1000000
            or not 1 <= page_size <= 20
        ):
            raise QueryError("量化尝试分页范围无效")
        value = await self._read(
            f"/v1/research-runs/{run_id}/quant-attempts",
            QuantAttemptPage,
            {"page": str(page), "page_size": str(page_size)},
        )
        if value.run_id != run_id or (value.page, value.page_size) != (page, page_size):
            raise QueryError("量化尝试与指定运行不一致")
        return value
