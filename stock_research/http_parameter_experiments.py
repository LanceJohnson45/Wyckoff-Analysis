"""私有参数实验作业动作与SQL分页读取；GET不启动计算。"""

from typing import Annotated

from fastapi import APIRouter, Header, Query, Request
from pydantic import UUID4

from stock_research.evaluation_store import EvaluationStore, parameter_job_view
from stock_research.parameter_experiment_results import parameter_experiment_result
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.parameter_experiment_jobs import (
    CreateParameterExperiment,
    ParameterExperimentJobPage,
    ParameterExperimentJobView,
    ParameterExperimentResultView,
)

router = APIRouter()


@router.post("/v1/parameter-experiments", status_code=202, response_model=ApiResponse[ParameterExperimentJobView])
async def create(
    request: Request,
    body: CreateParameterExperiment,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[ParameterExperimentJobView]:
    identifier = await request.app.state.evaluations.submit(key, body)
    row = await EvaluationStore(request.app.state.store).read(identifier)
    return ApiResponse(
        code=Code.OK,
        message="accepted",
        data=parameter_job_view(row),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/parameter-experiments", response_model=ApiResponse[ParameterExperimentJobPage])
async def jobs(
    request: Request,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ApiResponse[ParameterExperimentJobPage]:
    data = await EvaluationStore(request.app.state.store).experiment_page(page, page_size)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/parameter-experiments/{identifier}", response_model=ApiResponse[ParameterExperimentJobView])
async def job(request: Request, identifier: UUID4) -> ApiResponse[ParameterExperimentJobView]:
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=parameter_job_view(row), correlation_id=request.state.correlation_id
    )


@router.post("/v1/parameter-experiments/{identifier}/cancel", response_model=ApiResponse[ParameterExperimentJobView])
async def cancel(request: Request, identifier: UUID4) -> ApiResponse[ParameterExperimentJobView]:
    await request.app.state.evaluations.cancel(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="ok", data=parameter_job_view(row), correlation_id=request.state.correlation_id
    )


@router.post(
    "/v1/parameter-experiments/{identifier}/retry",
    status_code=202,
    response_model=ApiResponse[ParameterExperimentJobView],
)
async def retry(request: Request, identifier: UUID4) -> ApiResponse[ParameterExperimentJobView]:
    await request.app.state.evaluations.retry(str(identifier))
    row = await EvaluationStore(request.app.state.store).read(str(identifier))
    return ApiResponse(
        code=Code.OK, message="accepted", data=parameter_job_view(row), correlation_id=request.state.correlation_id
    )


@router.get(
    "/v1/parameter-experiment-results/{identifier}",
    response_model=ApiResponse[ParameterExperimentResultView],
)
async def result(request: Request, identifier: UUID4) -> ApiResponse[ParameterExperimentResultView]:
    data = await parameter_experiment_result(request.app.state.store, str(identifier))
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)
