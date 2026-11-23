"""工作台人工输入维护，模型工具没有这些写入口或operator身份。"""

from typing import Annotated, Literal

from fastapi import APIRouter, Header, Query, Request
from pydantic import UUID4, AfterValidator

from stock_research.input_configuration import frozen_inputs
from stock_research.input_maintenance import InputMaintenance
from stock_research.input_queries import input_publications, manual_head, theme_proposals
from stock_research.models import normalize_symbol
from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.input_maintenance import (
    ConfirmTheme,
    CreateManualOverride,
    FrozenInputs,
    InputPublication,
    InputPublicationPage,
    ManualHead,
    ThemeDefinition,
    ThemeProposal,
    ThemeProposalPage,
)
from stock_research.source_guard import ensure_source_current

router = APIRouter()


@router.post("/v1/theme-proposals", status_code=201, response_model=ApiResponse[ThemeProposal])
async def propose(
    request: Request,
    body: ThemeDefinition,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[ThemeProposal]:
    ensure_source_current()
    data = await InputMaintenance(request.app.state.store, request.app.state.settings).propose(key, body)
    return ApiResponse(code=Code.OK, message="created", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/theme-proposals", response_model=ApiResponse[ThemeProposalPage])
async def proposals(
    request: Request,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=20)] = 10,
    status: Literal["pending", "confirmed"] | None = None,
    name: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> ApiResponse[ThemeProposalPage]:
    data = await theme_proposals(request.app.state.store, request.app.state.settings, page, page_size, status, name)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/theme-proposals/{identifier}", response_model=ApiResponse[ThemeProposal])
async def proposal(request: Request, identifier: UUID4) -> ApiResponse[ThemeProposal]:
    data = await InputMaintenance(request.app.state.store, request.app.state.settings).proposal(str(identifier))
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.post("/v1/theme-proposals/{identifier}/confirm", response_model=ApiResponse[InputPublication])
async def confirm(request: Request, identifier: UUID4, body: ConfirmTheme) -> ApiResponse[InputPublication]:
    ensure_source_current()
    data = await InputMaintenance(request.app.state.store, request.app.state.settings).confirm(str(identifier), body)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/input-publications", response_model=ApiResponse[InputPublicationPage])
async def publications(
    request: Request,
    kind: Literal["theme", "manual"] = "theme",
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=20)] = 10,
    target: Annotated[str | None, Query(min_length=1, max_length=512)] = None,
) -> ApiResponse[InputPublicationPage]:
    data = await input_publications(request.app.state.store, page, page_size, kind, target)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.post("/v1/universe-overrides", status_code=201, response_model=ApiResponse[InputPublication])
async def override(
    request: Request,
    body: CreateManualOverride,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[InputPublication]:
    ensure_source_current()
    data = await InputMaintenance(request.app.state.store, request.app.state.settings).override(key, body)
    return ApiResponse(code=Code.OK, message="created", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/universe-overrides", response_model=ApiResponse[InputPublicationPage])
async def overrides(
    request: Request,
    page: Annotated[int, Query(ge=1, le=1_000_000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=20)] = 10,
) -> ApiResponse[InputPublicationPage]:
    data = await input_publications(request.app.state.store, page, page_size, "manual")
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/universe-overrides/{symbol}", response_model=ApiResponse[ManualHead])
async def current_manual(
    request: Request, symbol: Annotated[str, AfterValidator(normalize_symbol)]
) -> ApiResponse[ManualHead]:
    data = await manual_head(request.app.state.store, symbol)
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)


@router.get("/v1/research-runs/{run_id}/inputs", response_model=ApiResponse[FrozenInputs])
async def frozen(request: Request, run_id: UUID4) -> ApiResponse[FrozenInputs]:
    data = FrozenInputs(run_id=run_id, inputs=await frozen_inputs(request.app.state.store, str(run_id)))
    return ApiResponse(code=Code.OK, message="ok", data=data, correlation_id=request.state.correlation_id)
