"""固定报告的研究只读端点，报告身份限定workspace和历史修订。"""

from typing import Annotated, Literal

from fastapi import APIRouter, Query, Request
from pydantic import UUID4

from stock_research.report_queries import compare_stocks, group_strength, market_overview, strategy_rules
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.report_queries import GroupPage, MarketOverview, StockComparison, StrategyRules, Symbol

router = APIRouter()


@router.get("/v1/reports/{report_id}/market", response_model=ApiResponse[MarketOverview])
async def market(
    request: Request, report_id: UUID4, limit: Annotated[int, Query(ge=1, le=320)] = 60
) -> ApiResponse[MarketOverview]:
    """查询原市场事实与三基准历史，不推导执行许可。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await market_overview(request.app.state.store, str(report_id), limit),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/reports/{report_id}/groups", response_model=ApiResponse[GroupPage])
async def groups(
    request: Request,
    report_id: UUID4,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    kind: Literal["industry", "theme"] | None = None,
    name: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
) -> ApiResponse[GroupPage]:
    """组名包含industry/theme前缀，分数缺失继续显示未知。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await group_strength(request.app.state.store, str(report_id), page, page_size, kind, name),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/reports/{report_id}/compare", response_model=ApiResponse[StockComparison])
async def compare(
    request: Request,
    report_id: UUID4,
    symbols: Annotated[list[Symbol], Query(min_length=2, max_length=8)],
    limit: Annotated[int, Query(ge=1, le=60)] = 20,
) -> ApiResponse[StockComparison]:
    """重复symbols查询参数，禁止混入别的快照或重复代码。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await compare_stocks(request.app.state.store, str(report_id), tuple(symbols), limit),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/reports/{report_id}/rules", response_model=ApiResponse[StrategyRules])
async def rules(request: Request, report_id: UUID4) -> ApiResponse[StrategyRules]:
    """读取当时参数、工程定义和有效配置。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await strategy_rules(request.app.state.store, str(report_id)),
        correlation_id=request.state.correlation_id,
    )
