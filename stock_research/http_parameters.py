"""参数版本维护与当前/冻结配置查询，不提供报告初选覆盖。"""

from typing import Annotated

from fastapi import FastAPI, Query, Request
from pydantic import UUID4

from stock_research.parameter_config import current_parameters, frozen_parameters
from stock_research.parameter_store import activate_parameters, list_parameters, parameter_view, register_parameters
from stock_research.schema import PARAMETER_SETS
from stock_research.schemas.api import ApiResponse, Code, Strategy
from stock_research.schemas.parameters import (
    ActivateParameters,
    CreateParameterSet,
    ParameterSetPage,
    ParameterSetView,
    StrategyParameterList,
    StrategyParameterView,
)
from stock_research.source_guard import ensure_source_current


def register_parameter_reads(app: FastAPI) -> None:
    """当前配置与固定运行配置是不同查询，避免拿新参数改写旧报告。"""

    @app.get("/v1/strategies", response_model=ApiResponse[StrategyParameterList])
    async def strategies(request: Request) -> ApiResponse[StrategyParameterList]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await current_parameters(request.app.state.store, request.app.state.settings),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/research-runs/{run_id}/parameters", response_model=ApiResponse[StrategyParameterList])
    async def frozen(request: Request, run_id: UUID4) -> ApiResponse[StrategyParameterList]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await frozen_parameters(request.app.state.store, str(run_id)),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/parameter-sets", response_model=ApiResponse[ParameterSetPage])
    async def profiles(
        request: Request,
        strategy: Strategy | None = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> ApiResponse[ParameterSetPage]:
        return ApiResponse(
            code=Code.OK,
            message="ok",
            data=await list_parameters(request.app.state.store, page, page_size, strategy),
            correlation_id=request.state.correlation_id,
        )

    @app.get("/v1/parameter-sets/{parameter_id}", response_model=ApiResponse[ParameterSetView])
    async def profile(request: Request, parameter_id: UUID4) -> ApiResponse[ParameterSetView]:
        row = await request.app.state.store.row(PARAMETER_SETS, str(parameter_id))
        return ApiResponse(
            code=Code.OK, message="ok", data=parameter_view(row), correlation_id=request.state.correlation_id
        )


def register_parameter_actions(app: FastAPI) -> None:
    """独立只读身份在中间件拒绝所有动作；发布仅消费已注册参数。"""

    @app.post("/v1/parameter-sets", status_code=201, response_model=ApiResponse[ParameterSetView])
    async def create(request: Request, body: CreateParameterSet) -> ApiResponse[ParameterSetView]:
        ensure_source_current()
        return ApiResponse(
            code=Code.OK,
            message="created",
            data=await register_parameters(request.app.state.store, body),
            correlation_id=request.state.correlation_id,
        )

    @app.post("/v1/strategies/{strategy}/activate-parameters", response_model=ApiResponse[StrategyParameterView])
    async def activate(
        request: Request, strategy: Strategy, body: ActivateParameters
    ) -> ApiResponse[StrategyParameterView]:
        ensure_source_current()
        data = await activate_parameters(
            request.app.state.store, strategy, str(body.parameter_set_id), body.expected_revision
        )
        return ApiResponse(code=Code.OK, message="activated", data=data, correlation_id=request.state.correlation_id)
