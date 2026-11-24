"""私有评估作业动作与SQL分页读取，GET不启动计算。"""

from typing import Annotated

from fastapi import APIRouter, Header, Query, Request
from pydantic import UUID4

from stock_research.evaluation_results import evaluation_result, sample_page
from stock_research.evaluation_store import EvaluationStore, evaluation_view
from stock_research.schemas.api import ApiResponse, Code, Strategy
from stock_research.schemas.evaluations import (
    CreateEvaluation,
    EvaluationJobPage,
    EvaluationJobView,
    EvaluationResultView,
    EvaluationSamplePage,
)

router = APIRouter()


@router.post("/v1/evaluations", status_code=202, response_model=ApiResponse[EvaluationJobView])
async def create(
    request: Request,
    body: CreateEvaluation,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[EvaluationJobView]:
    identifier = await request.app.state.evaluations.submit(key, body)
    row = await EvaluationStore(request.app.state.store).read(identifier)
    return ApiResponse(
        code=Code.OK, message="accepted", data=evaluation_view(row), correlation_id=request.state.correlation_id
    )


@router.get("/v1/evaluations", response_model=ApiResponse[EvaluationJobPage])
async def jobs(
    request: Request,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ApiResponse[EvaluationJobPage]:
    data = await EvaluationStore(request.app.state.store).page(page, page_size)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/evaluations/{identifier}", response_model=ApiResponse[EvaluationJobView])
async def job(request: Request, identifier: UUID4) -> ApiResponse[EvaluationJobView]:
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=evaluation_view(row), correlation_id=request.state.correlation_id
    )


@router.post("/v1/evaluations/{identifier}/cancel", response_model=ApiResponse[EvaluationJobView])
async def cancel(request: Request, identifier: UUID4) -> ApiResponse[EvaluationJobView]:
    await request.app.state.evaluations.cancel(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=evaluation_view(row), correlation_id=request.state.correlation_id
    )


@router.post("/v1/evaluations/{identifier}/retry", status_code=202, response_model=ApiResponse[EvaluationJobView])
async def retry(request: Request, identifier: UUID4) -> ApiResponse[EvaluationJobView]:
    await request.app.state.evaluations.retry(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="accepted", data=evaluation_view(row), correlation_id=request.state.correlation_id
    )


@router.get("/v1/evaluation-results/{identifier}", response_model=ApiResponse[EvaluationResultView])
async def result(request: Request, identifier: UUID4) -> ApiResponse[EvaluationResultView]:
    data = await evaluation_result(request.app.state.store, str(identifier))
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/evaluation-results/{identifier}/samples", response_model=ApiResponse[EvaluationSamplePage])
async def samples(
    request: Request,
    identifier: UUID4,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    strategy: Strategy | None = None,
) -> ApiResponse[EvaluationSamplePage]:
    data = await sample_page(request.app.state.store, str(identifier), page, page_size, strategy)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)
