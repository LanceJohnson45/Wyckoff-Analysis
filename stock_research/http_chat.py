"""私有研究问答，仅operator可读写，不复用共享行情只读身份。"""

from typing import Annotated

from fastapi import APIRouter, Header, Query, Request
from pydantic import UUID4

from stock_research.schemas.api import ApiResponse, Code
from stock_research.schemas.chat import ChatTurnPage, ChatTurnView, CreateChatTurn

router = APIRouter()


@router.post("/v1/chat-turns", status_code=202, response_model=ApiResponse[ChatTurnView])
async def create_turn(
    request: Request,
    body: CreateChatTurn,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=16, max_length=128)],
) -> ApiResponse[ChatTurnView]:
    """202只确认已落库，模型完成需查询对应问答状态。"""
    return ApiResponse(
        code=Code.OK,
        message="accepted",
        data=await request.app.state.chat.submit(idempotency_key, body),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/chat-turns", response_model=ApiResponse[ChatTurnPage])
async def turns(
    request: Request,
    report_id: UUID4,
    conversation_id: UUID4 | None = None,
    page: Annotated[int, Query(ge=1)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
) -> ApiResponse[ChatTurnPage]:
    """报告和可选会话过滤，未知/错误/空历史分别表达。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await request.app.state.chat.chats.page(
            str(report_id), page, page_size, str(conversation_id) if conversation_id else None
        ),
        correlation_id=request.state.correlation_id,
    )


@router.get("/v1/chat-turns/{turn_id}", response_model=ApiResponse[ChatTurnView])
async def turn(request: Request, turn_id: UUID4) -> ApiResponse[ChatTurnView]:
    """按workspace读取白名单，不返回原始SDK、内部路径或凭据。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await request.app.state.chat.chats.read(str(turn_id)),
        correlation_id=request.state.correlation_id,
    )


@router.post("/v1/chat-turns/{turn_id}/cancel", response_model=ApiResponse[ChatTurnView])
async def cancel_turn(request: Request, turn_id: UUID4) -> ApiResponse[ChatTurnView]:
    """显式取消不创建新的问题或覆盖已经提交的回答。"""
    return ApiResponse(
        code=Code.OK,
        message="ok",
        data=await request.app.state.chat.cancel(str(turn_id)),
        correlation_id=request.state.correlation_id,
    )
