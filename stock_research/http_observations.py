"""股票只读查询，不接受数据源或执行配置。"""

from typing import Annotated

from fastapi import FastAPI, Path, Query, Request
from pydantic import UUID4

from stock_research.observations import stock_observation
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.observations import StockObservation


def register_observations(app: FastAPI) -> None:
    """固定workspace由服务端身份持有，run限定快照和策略版本。"""

    @app.get("/v1/stocks/{symbol}/observations", response_model=ApiResponse[StockObservation])
    async def stock(
        request: Request,
        symbol: Annotated[str, Path(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")],
        run_id: UUID4,
        limit: Annotated[int, Query(ge=1, le=320)] = 60,
        report_id: UUID4 | None = None,
    ) -> ApiResponse[StockObservation]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await stock_observation(
                request.app.state.store, str(run_id), symbol, limit, str(report_id) if report_id else None
            ),
            correlation_id=request.state.correlation_id,
        )
