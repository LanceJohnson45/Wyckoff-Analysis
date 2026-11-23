"""研究输入准备回执查询；不提供修改或重写失败记录的端点。"""

from datetime import date
from typing import Annotated

from fastapi import FastAPI, Query, Request

from stock_research.preparations import preparation_page
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.preparations import PreparationPage


def register_preparations(app: FastAPI) -> None:
    """只读身份也可核对目标日准备状态，与模型执行账本分别表达。"""

    @app.get("/v1/preparations", response_model=ApiResponse[PreparationPage])
    async def preparations(
        request: Request,
        target_session: date | None = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> ApiResponse[PreparationPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await preparation_page(request.app.state.store, page, page_size, target_session),
            correlation_id=request.state.correlation_id,
        )
