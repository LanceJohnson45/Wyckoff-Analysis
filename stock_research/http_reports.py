"""固定报告读取及历史修订列表，不覆盖当时的研究视图。"""

from datetime import date
from typing import Annotated

from fastapi import FastAPI, Query, Request
from pydantic import UUID4

from stock_research.queries import latest_report_id, list_reports, report_view
from stock_research.schemas.api import ApiResponse, Code, ReportPage, ReportView


def register_reports(app: FastAPI) -> None:
    """读路径保留UUID、workspace及数据库分页约束。"""

    @app.get("/v1/reports/latest", response_model=ApiResponse[ReportView])
    async def latest(
        request: Request, target_session: date | None = None, run_id: UUID4 | None = None
    ) -> ApiResponse[ReportView]:
        store = request.app.state.store
        report_id = await latest_report_id(store, target_session, str(run_id) if run_id else None)
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await report_view(store, report_id),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/reports/{report_id}", response_model=ApiResponse[ReportView])
    async def report(request: Request, report_id: UUID4) -> ApiResponse[ReportView]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await report_view(request.app.state.store, str(report_id)),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/research-runs/{run_id}/reports", response_model=ApiResponse[ReportPage])
    async def history(
        request: Request,
        run_id: UUID4,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> ApiResponse[ReportPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await list_reports(request.app.state.store, str(run_id), page, page_size),
            correlation_id=request.state.correlation_id,
        )
