"""私有正式初选观测作业与结果路由。"""

from typing import Annotated

from fastapi import APIRouter, Header, Query, Request
from pydantic import UUID4

from stock_research.evaluation_store import EvaluationStore, observation_job_view
from stock_research.schemas.api import ApiResponse, Code, Strategy
from stock_research.schemas.selection_observation_jobs import (
    CreateSelectionObservation,
    SelectionObservationJobPage,
    SelectionObservationJobView,
    SelectionObservationResultView,
    SelectionObservationSamplePage,
)
from stock_research.selection_observation_results import (
    selection_observation_result,
    selection_observation_sample_page,
)

router = APIRouter()


@router.post("/v1/selection-observations", status_code=202, response_model=ApiResponse[SelectionObservationJobView])
async def create(
    request: Request,
    body: CreateSelectionObservation,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[SelectionObservationJobView]:
    identifier = await request.app.state.evaluations.submit(key, body)
    row = await EvaluationStore(request.app.state.store).read(identifier)
    return ApiResponse(
        code=Code.OK, message="accepted", data=observation_job_view(row), correlation_id=request.state.correlation_id
    )


@router.get("/v1/selection-observations", response_model=ApiResponse[SelectionObservationJobPage])
async def jobs(
    request: Request,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ApiResponse[SelectionObservationJobPage]:
    data = await EvaluationStore(request.app.state.store).observation_page(page, page_size)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/selection-observations/{identifier}", response_model=ApiResponse[SelectionObservationJobView])
async def job(request: Request, identifier: UUID4) -> ApiResponse[SelectionObservationJobView]:
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=observation_job_view(row), correlation_id=request.state.correlation_id
    )


@router.post("/v1/selection-observations/{identifier}/cancel", response_model=ApiResponse[SelectionObservationJobView])
async def cancel(request: Request, identifier: UUID4) -> ApiResponse[SelectionObservationJobView]:
    await request.app.state.evaluations.cancel(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=observation_job_view(row), correlation_id=request.state.correlation_id
    )


@router.post(
    "/v1/selection-observations/{identifier}/retry",
    status_code=202,
    response_model=ApiResponse[SelectionObservationJobView],
)
async def retry(request: Request, identifier: UUID4) -> ApiResponse[SelectionObservationJobView]:
    await request.app.state.evaluations.retry(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="accepted", data=observation_job_view(row), correlation_id=request.state.correlation_id
    )


@router.get(
    "/v1/selection-observation-results/{identifier}", response_model=ApiResponse[SelectionObservationResultView]
)
async def result(request: Request, identifier: UUID4) -> ApiResponse[SelectionObservationResultView]:
    data = await selection_observation_result(request.app.state.store, str(identifier))
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get(
    "/v1/selection-observation-results/{identifier}/samples", response_model=ApiResponse[SelectionObservationSamplePage]
)
async def samples(
    request: Request,
    identifier: UUID4,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    strategy: Strategy | None = None,
) -> ApiResponse[SelectionObservationSamplePage]:
    data = await selection_observation_sample_page(request.app.state.store, str(identifier), page, page_size, strategy)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)
