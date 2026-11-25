"""研究API认证与错误翻译；不共享Personal身份或输出内部异常。"""

import re
import secrets
from collections.abc import Awaitable, Callable
from ipaddress import ip_address

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException

from stock_research.config import Settings
from stock_research.errors import (
    ResearchConflict,
    ResearchInputError,
    ResearchIntegrityError,
    ResearchNotFound,
    ResearchSourceChanged,
    ResearchUnavailable,
)
from stock_research.identity import new_id
from stock_research.schemas.api import ApiResponse, Code

logger = structlog.get_logger(__name__)


def trusted_tailscale(request: Request, settings: Settings) -> bool:
    """只接受loopback上的Tailscale Serve反代及其显式白名单身份。"""
    host = settings.tailscale_serve_host
    if not host or request.client is None or request.client.host not in {"127.0.0.1", "::1"}:
        return False
    headers = {
        name: request.headers.getlist(name)
        for name in ("x-forwarded-for", "x-forwarded-proto", "x-forwarded-host", "tailscale-user-login")
    }
    if any(len(values) != 1 for values in headers.values()):
        return False
    try:
        ip_address(headers["x-forwarded-for"][0])
    except ValueError:
        return False
    actual_host = request.headers.get("host", "").lower()
    target_hosts = {host, f"127.0.0.1:{settings.api_port}"}
    return (
        headers["x-forwarded-proto"][0].lower() == "https"
        and headers["x-forwarded-host"][0].lower() == host
        and len(request.headers.getlist("host")) == 1
        and actual_host in target_hosts
        and secrets.compare_digest(headers["tailscale-user-login"][0], settings.tailscale_serve_login)
        and request.headers.get("sec-fetch-site", "none") in {"same-origin", "none"}
        and request.headers.get("origin") in {None, settings.tailscale_serve_origin}
        and (request.method in {"GET", "HEAD"} or request.headers.get("origin") is not None)
    )


def error_response(request: Request, status: int, code: Code, message: str) -> JSONResponse:
    """错误不反射入参、驱动信息、文件路径或凭据。"""
    body = ApiResponse[None](code=code, message=message, data=None, correlation_id=request.state.correlation_id)
    return JSONResponse(status_code=status, content=body.model_dump(mode="json"), headers={"Cache-Control": "no-store"})


def install_security(app: FastAPI, settings: Settings) -> None:
    """Bearer为必需身份；Origin校验另阻止浏览器跨站调用。"""
    token = settings.api_token.get_secret_value() if settings.api_token else ""
    if len(token) < 32 or token != token.strip():
        raise ValueError("研究API须配置至少32字符的独立STOCK_RESEARCH_API_TOKEN")
    read_token = settings.api_read_token.get_secret_value() if settings.api_read_token else ""
    if read_token and (len(read_token) < 32 or read_token != read_token.strip() or read_token == token):
        raise ValueError("研究只读身份须独立且至少32字符")
    workflow_token = settings.api_workflow_token.get_secret_value() if settings.api_workflow_token else ""
    if workflow_token and (
        len(workflow_token) < 32
        or workflow_token != workflow_token.strip()
        or workflow_token in {token, read_token}
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", settings.premarket_source_workspace)
    ):
        raise ValueError("盘前身份须独立且至少32字符，并固定来源工作区")

    @app.middleware("http")
    async def authentication(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.correlation_id = new_id()
        actual_host = request.headers.get("host", "")
        tailscale = trusted_tailscale(request, settings)
        if actual_host.split(":", 1)[0] not in {"127.0.0.1", "localhost"} and not tailscale:
            return error_response(request, 403, Code.FORBIDDEN, "研究API只接受本机Host")
        origin = request.headers.get("origin")
        if origin is not None and origin not in settings.api_allowed_origins:
            return error_response(request, 403, Code.FORBIDDEN, "浏览器来源未授权")
        if request.url.path not in {"/healthz", "/readyz"}:
            authorization = request.headers.get("authorization", "")
            operator = secrets.compare_digest(authorization.encode(), f"Bearer {token}".encode())
            reader = bool(read_token) and secrets.compare_digest(
                authorization.encode(), f"Bearer {read_token}".encode()
            )
            workflow = bool(workflow_token) and secrets.compare_digest(
                authorization.encode(), f"Bearer {workflow_token}".encode()
            )
            if not operator and not reader and not workflow:
                response = error_response(request, 401, Code.UNAUTHENTICATED, "需要研究API身份")
                response.headers["WWW-Authenticate"] = "Bearer"
                return response
            private = request.url.path.startswith(
                (
                    "/v1/chat-turns",
                    "/v1/evaluations",
                    "/v1/evaluation-results",
                    "/v1/selection-observations",
                    "/v1/selection-observation-results",
                    "/v1/parameter-experiments",
                    "/v1/parameter-experiment-results",
                    "/v1/theme-proposals",
                    "/v1/input-publications",
                    "/v1/universe-overrides",
                    "/v1/premarket-jobs",
                    "/v1/operations",
                )
            ) or request.url.path.endswith("/inputs")
            if workflow and not workflow_route(request.method, request.url.path):
                return error_response(request, 403, Code.FORBIDDEN, "此研究身份仅允许盘前任务")
            if reader and (request.method != "GET" or private):
                return error_response(request, 403, Code.FORBIDDEN, "此研究身份仅允许查询")
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Correlation-Id"] = request.state.correlation_id
        return response

    install_error_handlers(app)


def workflow_route(method: str, path: str) -> bool:
    """前缀相似的路径不能变成普通研究、私有数据或额外写能力。"""
    if path == "/v1/premarket-jobs":
        return method == "POST"
    match = re.fullmatch(r"/v1/premarket-jobs/[A-Fa-f0-9-]{36}(/retry|/cancel)?", path)
    return bool(match) and method == ("POST" if match.group(1) else "GET")


def install_error_handlers(app: FastAPI) -> None:
    """领域错误在HTTP边界翻译，不向客户端暴露驱动或路径。"""

    async def not_found(request: Request, _error: ResearchNotFound) -> JSONResponse:
        return error_response(request, 404, Code.NOT_FOUND, "研究对象不存在")

    async def conflict(request: Request, _error: ResearchConflict) -> JSONResponse:
        return error_response(request, 409, Code.CONFLICT, "研究状态或版本冲突，请刷新后重试")

    async def invalid(request: Request, _error: RequestValidationError) -> JSONResponse:
        return error_response(request, 422, Code.INVALID, "请求参数不符合研究契约")

    async def unavailable(request: Request, _error: Exception) -> JSONResponse:
        return error_response(request, 503, Code.UNAVAILABLE, "研究存储或输入来源暂不可用")

    async def source_changed(request: Request, _error: ResearchSourceChanged) -> JSONResponse:
        return error_response(request, 503, Code.UNAVAILABLE, "研究代码已更新，请重启研究服务后重试；旧报告仍可读取")

    async def internal(request: Request, error: Exception) -> JSONResponse:
        logger.error("研究API内部错误", error_type=type(error).__name__, correlation_id=request.state.correlation_id)
        return error_response(request, 500, Code.INTERNAL, "研究服务内部错误")

    async def protocol(request: Request, error: HTTPException) -> JSONResponse:
        return error_response(
            request, error.status_code, Code.NOT_FOUND if error.status_code == 404 else Code.INVALID, "HTTP请求无效"
        )

    for exception, handler in (
        (ResearchNotFound, not_found),
        (ResearchConflict, conflict),
        (RequestValidationError, invalid),
        (ResearchInputError, invalid),
        (SQLAlchemyError, unavailable),
        (ResearchIntegrityError, unavailable),
        (ResearchUnavailable, unavailable),
        (ResearchSourceChanged, source_changed),
        (TimeoutError, unavailable),
        (OSError, unavailable),
        (HTTPException, protocol),
        (Exception, internal),
    ):
        app.add_exception_handler(exception, handler)
