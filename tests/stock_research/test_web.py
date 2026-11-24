"""同进程静态入口和cookie桥接的真实临时PG验收。"""

import asyncio
from pathlib import Path

import httpx
import pytest

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.schema import PARAMETER_SETS
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
TOKEN = "synthetic-static-web-operator-00000000001"
BASE = "/api/stock-research"
ORIGIN = {"Origin": "http://127.0.0.1:8788", "Sec-Fetch-Site": "same-origin"}


@pytest.fixture
def web_settings(postgres_settings: Settings, tmp_path: Path) -> Settings:
    """最小产物只检验文件/认证边界，完整构建和真实页面另验。"""
    root = tmp_path / "web"
    (root / "assets").mkdir(parents=True)
    (root / "research.html").write_text(
        '<meta name="stock-research-entry" content="1"><script type="module" src="/assets/main-test.js"></script>'
    )
    (root / "assets/main-test.js").write_text("const synthetic = true")
    return Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(),
            "postgres_database": "stock_research_test",
            "workspace": "static-web-" + new_id(),
            "artifacts_path": tmp_path / "artifacts",
            "api_token": TOKEN,
            "web_dist_path": root.resolve(),
        },
    )


async def initialize(settings: Settings) -> None:
    """显式建立临时簇schema，不借启动静态站点迁移数据库。"""
    engine = database_engine(settings)
    try:
        await initialize_schema(engine)
    finally:
        await close_engine(engine)


def test_integration_static_web_cookie_reads_and_writes_original_api(web_settings: Settings) -> None:
    """[INTEGRATION] 静态入口签发cookie，白名单桥接真实读写但直连仍须Bearer。"""

    async def scenario() -> None:
        await initialize(web_settings)
        app = create_app(web_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            missing = await client.get(BASE + "/v1/research-runs")
            assert missing.status_code == 401 and missing.headers["x-research-web-session"] == "required"
            page = await client.get("/chat")
            assert page.status_code == 200 and TOKEN not in page.text
            assert page.headers["x-research-web-session"] == "ready"
            assert "HttpOnly" in page.headers["set-cookie"] and "SameSite=strict" in page.headers["set-cookie"]
            assert "Path=/api/stock-research" in page.headers["set-cookie"]
            assert page.headers["cache-control"] == "no-store"
            assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
            asset = await client.get("/assets/main-test.js")
            assert asset.status_code == 200 and "javascript" in asset.headers["content-type"]
            assert (await client.get("/v1/research-runs")).status_code == 401
            runs = await client.get(BASE + "/v1/research-runs", headers=ORIGIN)
            assert runs.status_code == 200 and runs.json()["data"]["total"] == 0
            body = {
                "strategy": "martin",
                "parameters": MartinParameters(version="static-web-v1").model_dump(mode="json"),
            }
            saved = await client.post(BASE + "/v1/parameter-sets", json=body, headers=ORIGIN)
            assert saved.status_code == 201, saved.text
            identifier = saved.json()["data"]["id"]
            assert (await app.state.store.row(PARAMETER_SETS, identifier))["payload"]["parameters"][
                "version"
            ] == "static-web-v1"
            direct = await client.get("/v1/parameter-sets/" + identifier, headers={"Authorization": "Bearer " + TOKEN})
            assert direct.status_code == 200 and direct.json()["data"] == saved.json()["data"]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("headers", "path", "method", "status"),
    [
        pytest.param(
            {"Origin": "https://untrusted.invalid"}, BASE + "/v1/parameter-sets", "POST", 403, id="foreign-origin"
        ),
        pytest.param({}, BASE + "/v1/parameter-sets", "POST", 403, id="missing-write-origin"),
        pytest.param(
            {**ORIGIN, "Host": "untrusted.invalid"}, BASE + "/v1/parameter-sets", "POST", 403, id="foreign-host"
        ),
        pytest.param({**ORIGIN, "Host": "localhost:9999"}, BASE + "/v1/parameter-sets", "POST", 403, id="wrong-port"),
        pytest.param(
            {**ORIGIN, "Sec-Fetch-Site": "cross-site"}, BASE + "/v1/parameter-sets", "POST", 403, id="cross-site"
        ),
        pytest.param(ORIGIN, BASE + "/v1/premarket-jobs", "POST", 404, id="premarket-write"),
        pytest.param(ORIGIN, BASE + "/v1/watchlists", "POST", 404, id="unsupported-write"),
        pytest.param(ORIGIN, BASE + "/v1/parameter-sets", "DELETE", 404, id="unsupported-method"),
    ],
)
def test_validation_static_web_rejects_untrusted_or_extra_actions(
    web_settings: Settings, headers: dict[str, str], path: str, method: str, status: int
) -> None:
    """[VALIDATION] 有会话也不能越过来源、方法及原浏览器白名单。"""

    async def scenario() -> None:
        await initialize(web_settings)
        app = create_app(web_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            assert (await client.get("/chat")).status_code == 200
            rejected = await client.request(method, path, headers=headers, json={})
            assert rejected.status_code == status and rejected.json()["data"] is None
            page = await client.get(BASE + "/v1/parameter-sets", headers=ORIGIN)
            assert page.json()["data"]["total"] == 0

    asyncio.run(scenario())


def test_integration_static_web_session_expiry_and_restart_keep_business_state(
    web_settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 过期/旧进程会话只得到认证前401，入口重读后原API仍可用。"""
    from time import time

    from stock_research.web_session import SESSION_SECONDS

    async def scenario() -> None:
        await initialize(web_settings)
        app = create_app(web_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            with monkeypatch.context() as clock:
                clock.setattr("stock_research.web_session.time", lambda: time() - SESSION_SECONDS - 1)
                assert (await client.get("/chat")).status_code == 200
            expired = await client.get(BASE + "/v1/research-runs", headers=ORIGIN)
            assert expired.status_code == 401 and expired.headers["x-research-web-session"] == "required"
            assert (await client.get("/chat")).status_code == 200
            assert (await client.get(BASE + "/v1/research-runs", headers=ORIGIN)).status_code == 200
            old_cookies = client.cookies
        restored = create_app(web_settings)
        async with (
            restored.router.lifespan_context(restored),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=restored), base_url="http://127.0.0.1:8788", cookies=old_cookies
            ) as client,
        ):
            denied = await client.post(BASE + "/v1/parameter-sets", json={}, headers=ORIGIN)
            assert denied.status_code == 401 and denied.headers["x-research-web-session"] == "required"
            assert (await client.get("/chat")).status_code == 200
            page = await client.get(BASE + "/v1/parameter-sets", headers=ORIGIN)
            assert page.json()["data"]["total"] == 0

    asyncio.run(scenario())


def test_validation_static_web_rejects_tampering_body_overflow_and_symlink(
    web_settings: Settings, tmp_path: Path
) -> None:
    """[VALIDATION] 损坏会话/越界正文不写业务，资产及HTML不越过真实目录。"""

    async def scenario() -> None:
        await initialize(web_settings)
        app = create_app(web_settings)
        secret = tmp_path / "private.js"
        secret.write_text("synthetic-private-file-not-for-browser")
        (web_settings.web_dist_path / "assets/leak.js").symlink_to(secret)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            await client.get("/chat")
            name = "sr_web_8788"
            forged = {**ORIGIN, "Cookie": f"{name}=not-a-signed-session"}
            assert (await client.get(BASE + "/v1/parameter-sets", headers=forged)).status_code == 401
            overflow = await client.post(BASE + "/v1/parameter-sets", content=b"x" * 8193, headers=ORIGIN)
            assert overflow.status_code == 413
            for path in ("/assets/leak.js", "/assets/%2e%2e/private.js", "/assets/.env", "/assets/a%5cleak.js"):
                response = await client.get(path)
                assert response.status_code == 404 and secret.read_text() not in response.text
            (web_settings.web_dist_path / "research.html").unlink()
            (web_settings.web_dist_path / "research.html").symlink_to(secret)
            assert (await client.get("/chat")).status_code == 404
            page = await client.get(BASE + "/v1/parameter-sets", headers=ORIGIN)
            assert page.json()["data"]["total"] == 0

    asyncio.run(scenario())


def test_validation_static_web_requires_explicit_valid_build(web_settings: Settings, tmp_path: Path) -> None:
    """[VALIDATION] API-only不启用cookie，错误目录/公开构建/错误Origin配置拒启。"""

    async def api_only() -> None:
        config = web_settings.model_copy(update={"web_dist_path": None})
        await initialize(config)
        app = create_app(config)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            page = await client.get("/chat", headers={"Authorization": "Bearer " + TOKEN})
            assert page.status_code == 404 and "set-cookie" not in page.headers
            assert (await client.get(BASE + "/v1/research-runs")).status_code == 401

    asyncio.run(api_only())
    assert Settings(_env_file=None, web_dist_path="").web_dist_path is None
    with pytest.raises(ValueError, match="绝对目录"):
        create_app(web_settings.model_copy(update={"web_dist_path": Path("relative")}))
    with pytest.raises((ValueError, OSError)):
        create_app(web_settings.model_copy(update={"web_dist_path": tmp_path / "missing"}))
    with pytest.raises(ValueError, match="实际同源端口"):
        create_app(web_settings.model_copy(update={"api_port": 8793}))
    (web_settings.web_dist_path / "research.html").write_text('<script src="/src/main.tsx"></script>')
    with pytest.raises(ValueError, match="专用构建"):
        create_app(web_settings)


def test_unit_browser_session_signature_expiry_and_port_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    """[UNIT] 签名/未来时间/过期均拒收，不同进程及端口不共用浏览器身份。"""
    from stock_research.web_session import SESSION_SECONDS, BrowserSessions

    monkeypatch.setattr("stock_research.web_session.time", lambda: 1000)
    sessions = BrowserSessions(8788)
    token = sessions.issue()
    assert sessions.valid(token)
    assert not sessions.valid(token[:-1] + ("0" if token[-1] != "0" else "1"))
    assert not BrowserSessions(8788).valid(token)
    assert BrowserSessions(8793).name != sessions.name
    for damaged in ("", "..", "not-a-session", "é" * 128, "9" * 10000):
        assert not sessions.valid(damaged)
    monkeypatch.setattr("stock_research.web_session.time", lambda: 999)
    assert not sessions.valid(token)
    monkeypatch.setattr("stock_research.web_session.time", lambda: 1000 + SESSION_SECONDS)
    assert sessions.valid(token)
    monkeypatch.setattr("stock_research.web_session.time", lambda: 1001 + SESSION_SECONDS)
    assert not sessions.valid(token)


def test_validation_static_web_duplicate_host_and_head_do_not_issue_identity(web_settings: Settings) -> None:
    """[VALIDATION] 歧义Host拒收，HEAD不签发浏览器身份且不返回HTML正文。"""

    async def scenario() -> None:
        await initialize(web_settings)
        app = create_app(web_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            head = await client.head("/chat")
            assert head.status_code == 200 and head.content == b"" and "set-cookie" not in head.headers
            assert (await client.get(BASE + "/v1/parameter-sets", headers=ORIGIN)).status_code == 401
            rejected = await client.get("/chat", headers=[("Host", "127.0.0.1:8788"), ("Host", "untrusted.invalid")])
            assert rejected.status_code == 403 and "set-cookie" not in rejected.headers

    asyncio.run(scenario())


def test_integration_static_web_stream_preserves_original_report_and_cursor(
    web_settings: Settings, snapshot: Snapshot
) -> None:
    """[INTEGRATION] cookie桥接保留固定报告身份、SSE游标及断连接收通道。"""

    async def scenario() -> None:
        await initialize(web_settings)
        engine = database_engine(web_settings)
        try:
            created = await run_preview(
                engine, web_settings, snapshot, analyses_for(snapshot), snapshot.as_of.replace(day=23)
            )
        finally:
            await close_engine(engine)
        app = create_app(web_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            await client.get("/chat")
            saved = (await client.get(BASE + "/v1/reports/latest", headers=ORIGIN)).json()["data"]
            assert saved["run"]["run_id"] == created["run_id"]
            revision = saved["revision"]
            events = await client.get(
                BASE + f"/v1/research-runs/{created['run_id']}/events",
                headers={**ORIGIN, "Last-Event-ID": str(revision - 1)},
            )
            assert events.status_code == 200 and events.headers["content-type"].startswith("text/event-stream")
            assert events.text.count("event: report") == 1 and f"id: {revision}" in events.text
            empty = await client.get(
                BASE + f"/v1/research-runs/{created['run_id']}/events?after={revision}", headers=ORIGIN
            )
            assert "event: report" not in empty.text
            current = (await client.get(BASE + "/v1/reports/latest", headers=ORIGIN)).json()["data"]
            assert current == saved

    asyncio.run(scenario())
