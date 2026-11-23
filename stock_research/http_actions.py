"""固定业务动作；请求不获得参数文件、Shell或任意SQL能力。"""

from typing import Annotated

from fastapi import FastAPI, Header, Request
from pydantic import UUID4

from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.api import ApiResponse, Code, RunAccepted
from stock_research.schemas.requests import CreateRun


async def accepted(request: Request, run_id: str) -> ApiResponse[RunAccepted]:
    """公开运行引用与状态，不把受理当作研究成功。"""
    row = await request.app.state.store.row(RESEARCH_RUNS, run_id)
    return ApiResponse(
        code=Code.OK,
        message="accepted",
        data=RunAccepted(
            run_id=run_id,
            status=row["status"],
            status_url=f"/v1/research-runs/{run_id}",
            events_url=f"/v1/research-runs/{run_id}/events",
        ),
        correlation_id=request.state.correlation_id,
    )


def register_actions(app: FastAPI) -> None:
    """启动、失败续跑和取消保持独立动作及稳定HTTP状态。"""

    @app.post("/v1/research-runs", status_code=202, response_model=ApiResponse[RunAccepted])
    async def create(
        request: Request,
        body: CreateRun,
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
    ) -> ApiResponse[RunAccepted]:
        run_id = await request.app.state.manager.submit(idempotency_key, body)
        return await accepted(request, run_id)

    @app.post("/v1/research-runs/{run_id}/retry", status_code=202, response_model=ApiResponse[RunAccepted])
    async def retry(request: Request, run_id: UUID4) -> ApiResponse[RunAccepted]:
        await request.app.state.manager.retry(str(run_id))
        return await accepted(request, str(run_id))

    @app.post("/v1/research-runs/{run_id}/cancel", status_code=202, response_model=ApiResponse[RunAccepted])
    async def cancel(request: Request, run_id: UUID4) -> ApiResponse[RunAccepted]:
        await request.app.state.manager.cancel(str(run_id))
        return await accepted(request, str(run_id))
