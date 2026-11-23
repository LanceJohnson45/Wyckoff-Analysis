"""观察名单只读查询；研究初选及状态由流水线更新。"""

from typing import Annotated

from fastapi import FastAPI, Query, Request

from stock_research.schemas.api import ApiResponse, Code, Strategy
from stock_research.schemas.watchlists import WatchEventPage, WatchPage
from stock_research.watch_queries import event_page, watch_page


def register_watchlists(app: FastAPI) -> None:
    """名单与事件分页上限一百，不能由请求选择workspace。"""

    @app.get("/v1/watchlists", response_model=ApiResponse[WatchPage])
    async def items(
        request: Request,
        strategy: Strategy | None = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> ApiResponse[WatchPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await watch_page(request.app.state.store, page, page_size, strategy),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/watchlists/events", response_model=ApiResponse[WatchEventPage])
    async def events(
        request: Request,
        strategy: Strategy | None = None,
        symbol: Annotated[str | None, Query(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")] = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> ApiResponse[WatchEventPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await event_page(request.app.state.store, page, page_size, strategy, symbol),
            correlation_id=request.state.correlation_id,
        )
