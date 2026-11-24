"""同API进程的本机静态工作台与有会话的浏览器桥接。"""

import asyncio
from http.cookies import CookieError, SimpleCookie

from fastapi import FastAPI
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from stock_research.config import Settings
from stock_research.http_security import error_response
from stock_research.identity import new_id
from stock_research.schemas.api import Code
from stock_research.web_files import ResearchWebFiles
from stock_research.web_routing import BROWSER_BODY_LIMITS, allowed_browser_route
from stock_research.web_session import SESSION_SECONDS, BrowserSessions

WEB_BASE = "/api/stock-research"
CSP = (
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
    "connect-src 'self'; img-src 'self' data:; font-src 'self' data:; "
    "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
)


def install_web(app: FastAPI, settings: Settings) -> None:
    """未配置产物时不引入浏览器身份；资源错误在启动前明确失败。"""
    if settings.web_dist_path is not None:
        expected = "http://127.0.0.1" + (f":{settings.api_port}" if settings.api_port != 80 else "")
        if expected not in settings.api_allowed_origins:
            raise ValueError("研究静态入口须在API_ALLOWED_ORIGINS允许实际同源端口")
        files = ResearchWebFiles(settings.web_dist_path)
        app.add_middleware(LocalResearchWeb, settings=settings, files=files)


class LocalResearchWeb:
    """仅将受信浏览器的固定资源映射到原有鉴权/领域链。"""

    def __init__(self, app: ASGIApp, settings: Settings, files: ResearchWebFiles) -> None:
        self.app, self.settings, self.files = app, settings, files
        self.sessions = BrowserSessions(settings.api_port)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope["path"]
        bridge = path == WEB_BASE or path.startswith(WEB_BASE + "/")
        static = path in {"/", "/chat", "/chat/"} or path.startswith("/assets/")
        if not bridge and not static:
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        request.state.correlation_id = new_id()
        if not self.trusted(request):
            response = error_response(request, 403, Code.FORBIDDEN, "研究工作台来源未授权")
        elif bridge:
            await self.forward(request, receive, send)
            return
        else:
            response = await self.static(request)
        self.secure(response)
        await response(scope, receive, send)

    def trusted(self, request: Request) -> bool:
        """不信任转发头、其他端口或跨站导航；写操作必须有同源Origin。"""
        hosts = {f"127.0.0.1:{self.settings.api_port}", f"localhost:{self.settings.api_port}"}
        if self.settings.api_port == 80:
            hosts.update({"127.0.0.1", "localhost"})
        host = request.headers.get("host", "").lower()
        canonical_host = host.removesuffix(":80") if self.settings.api_port == 80 else host
        origin = request.headers.get("origin")
        return (
            request.url.scheme == "http"
            and len(request.headers.getlist("host")) == 1
            and host in hosts
            and request.headers.get("sec-fetch-site", "none") in {"same-origin", "none"}
            and (origin is None or origin == "http://" + canonical_host and origin in self.settings.api_allowed_origins)
            and (request.method in {"GET", "HEAD"} or origin is not None)
        )

    async def static(self, request: Request) -> Response:
        """只在既有/chat入口签发会话，不提供通配SPA或文件目录。"""
        try:
            if request.method not in {"GET", "HEAD"}:
                raise HTTPException(405)
            if request.url.path in {"/", "/chat/"}:
                return RedirectResponse("/chat", status_code=303)
            if request.url.path == "/chat":
                response = self.files.html()
                response.headers["X-Research-Web-Session"] = "ready"
                if request.method == "GET":
                    response.set_cookie(
                        self.sessions.name,
                        self.sessions.issue(),
                        path=WEB_BASE,
                        max_age=SESSION_SECONDS,
                        httponly=True,
                        samesite="strict",
                    )
                return response
            return await self.files.asset(request.url.path.lstrip("/"), request.scope)
        except (HTTPException, OSError) as error:
            status = error.status_code if isinstance(error, HTTPException) else 404
            return error_response(request, status, Code.NOT_FOUND, "研究静态资源不存在")

    async def forward(self, request: Request, receive: Receive, send: Send) -> None:
        """先验证再读取有界正文；凭据仅注入同进程的固定ASGI路径。"""
        path = request.url.path[len(WEB_BASE) :]
        if not allowed_browser_route(request.method, path):
            response = error_response(request, 404, Code.NOT_FOUND, "研究浏览器入口不存在")
        elif not self.valid_session(request):
            response = error_response(request, 401, Code.UNAUTHENTICATED, "工作台会话需更新")
            response.headers["X-Research-Web-Session"] = "required"
        else:
            try:
                body = await browser_body(receive, body_limit(request.method, path))
            except (ValueError, TimeoutError):
                response = error_response(request, 413, Code.INVALID, "研究浏览器请求超出限制")
            else:
                scope = {**request.scope, "path": path, "raw_path": path.encode()}
                headers = [
                    (key, value) for key, value in scope["headers"] if key.lower() not in {b"authorization", b"cookie"}
                ]
                token = self.settings.api_token.get_secret_value()
                scope["headers"] = [*headers, (b"authorization", ("Bearer " + token).encode())]
                await self.app(scope, replay_body(body, receive), send)
                return
        self.secure(response)
        await response(request.scope, receive, send)

    def valid_session(self, request: Request) -> bool:
        """损坏Cookie不能抛出500，也不能被当作另一种API身份。"""
        try:
            cookies = SimpleCookie(request.headers.get("cookie", ""))
            cookie = cookies.get(self.sessions.name)
            return cookie is not None and self.sessions.valid(cookie.value)
        except CookieError:
            return False

    @staticmethod
    def secure(response: Response) -> None:
        """静态入口不依赖公共站点的安全头或第三方脚本。"""
        response.headers.update(
            {"Content-Security-Policy": CSP, "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"}
        )


def body_limit(method: str, path: str) -> int:
    """沿用开发代理的正文边界，GET不携带业务正文。"""
    if method != "POST":
        return 0
    if path == "/v1/chat-turns":
        return BROWSER_BODY_LIMITS["chat"]
    return BROWSER_BODY_LIMITS["inputs" if path in {"/v1/theme-proposals", "/v1/universe-overrides"} else "default"]


async def browser_body(receive: Receive, maximum: int) -> bytes:
    """在调用领域动作前读完有界正文，超时/断连不能提交部分JSON。"""
    chunks, size = [], 0
    async with asyncio.timeout(20):
        while True:
            message = await receive()
            if message["type"] != "http.request":
                raise ValueError("浏览器请求已中断")
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > maximum:
                raise ValueError("正文越界")
            chunks.append(chunk)
            if not message.get("more_body", False):
                return b"".join(chunks)


def replay_body(body: bytes, receive: Receive) -> Receive:
    """回放已核验正文后仍使用真实断连通道，SSE取消不会被吞掉。"""
    pending = True

    async def replay() -> Message:
        nonlocal pending
        if pending:
            pending = False
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replay
