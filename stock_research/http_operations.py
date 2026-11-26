"""operator私有操作状态；GET只查询既有研究和通知账本。"""

from datetime import date

from fastapi import APIRouter, Request

from stock_research.errors import ResearchInputError
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.operations import OperationsView

router = APIRouter()


@router.get("/v1/operations", response_model=ApiResponse[OperationsView])
async def operations(request: Request, target_session: date) -> ApiResponse[OperationsView]:
    """目标日必填；配置来源、工作区及命令不能由查询方提供。"""
    if set(request.query_params) != {"target_session"} or len(request.query_params.getlist("target_session")) != 1:
        raise ResearchInputError("只允许一个目标交易日")
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await request.app.state.operations.view(target_session),
        correlation_id=request.state.correlation_id,
    )
