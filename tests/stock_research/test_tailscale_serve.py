"""显式Tailscale Serve入口的同源、HTTPS、身份和回退边界。"""

import asyncio
from pathlib import Path

import httpx
import pytest

from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from tests.stock_research.test_web import TOKEN, initialize

pytestmark = pytest.mark.usefixtures("local_postgres_socket")

HOST = "machine.tail5142a0.ts.net"
LOGIN = "operator@example.com"
ORIGIN = f"https://{HOST}"
BASE = "/api/stock-research"


@pytest.fixture
def web_settings(postgres_settings: Settings, tmp_path: Path) -> Settings:
    """与静态入口验收保持相同的最小专用产物和隔离workspace。"""
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


@pytest.fixture
def tailscale_settings(web_settings: Settings) -> Settings:
    """在既有静态入口夹具上显式开启一个固定Serve身份。"""
    return Settings(
        _env_file=None,
        **{
            **web_settings.model_dump(),
            "postgres_database": "stock_research_test",
            "workspace": "tailscale-" + new_id(),
            "tailscale_serve_host": HOST,
            "tailscale_serve_login": LOGIN,
            "api_allowed_origins": ("http://127.0.0.1:8788", "http://localhost:8788", ORIGIN),
        },
    )


def serve_headers(origin: str | None = None, **overrides: str) -> dict[str, str]:
    headers = {
        "X-Forwarded-For": "100.64.0.12",
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": HOST,
        "Tailscale-User-Login": LOGIN,
        "Sec-Fetch-Site": "none",
    }
    if origin is not None:
        headers["Origin"] = origin
        headers["Sec-Fetch-Site"] = "same-origin"
    headers.update(overrides)
    return headers


def test_normal_tailscale_serve_issues_secure_session_and_uses_original_api(
    tailscale_settings: Settings,
) -> None:
    """[INTEGRATION] Serve请求签发HTTPS Secure会话，桥接仍走原鉴权链。"""

    async def scenario() -> None:
        await initialize(tailscale_settings)
        app = create_app(tailscale_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 123)), base_url=ORIGIN
            ) as client,
        ):
            page = await client.get("/chat", headers=serve_headers())
            assert page.status_code == 200 and TOKEN not in page.text
            assert "Secure" in page.headers["set-cookie"] and "SameSite=strict" in page.headers["set-cookie"]
            assert (await client.get("/healthz")).status_code == 403
            assert (await client.get("/healthz", headers=serve_headers())).status_code == 200
            runs = await client.get(BASE + "/v1/research-runs", headers=serve_headers(ORIGIN))
            assert runs.status_code == 200 and runs.json()["data"]["total"] == 0
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 123)),
                base_url="http://127.0.0.1:8788",
            ) as local:
                local_page = await local.get("/chat")
                assert local_page.status_code == 200 and "Secure" not in local_page.headers["set-cookie"]
                assert (await local.get("/healthz")).status_code == 200

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("headers", "host", "status"),
    [
        pytest.param(serve_headers(), "untrusted.invalid", 403, id="foreign-host"),
        pytest.param(serve_headers("https://untrusted.invalid"), HOST, 403, id="foreign-origin"),
        pytest.param(serve_headers(**{"X-Forwarded-Proto": "http"}), HOST, 403, id="plain-forward"),
        pytest.param(serve_headers(**{"Tailscale-User-Login": "other@example.com"}), HOST, 403, id="foreign-login"),
        pytest.param(serve_headers(**{"Sec-Fetch-Site": "cross-site"}), HOST, 403, id="cross-site"),
        pytest.param(serve_headers(**{"X-Forwarded-For": "not-an-ip"}), HOST, 403, id="bad-forwarded-ip"),
    ],
)
def test_validation_tailscale_serve_rejects_bad_proxy_contract(
    tailscale_settings: Settings,
    headers: dict[str, str],
    host: str,
    status: int,
) -> None:
    """[VALIDATION] 主机、HTTPS、身份、Origin和Fetch-Site共同构成远端边界。"""

    async def scenario() -> None:
        await initialize(tailscale_settings)
        app = create_app(tailscale_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 123)),
                base_url=ORIGIN,
                headers={"Host": host},
            ) as client,
        ):
            assert (await client.get("/chat", headers=headers)).status_code == status
            assert (await client.get(BASE + "/v1/research-runs", headers=headers)).status_code == status


def test_validation_tailscale_serve_rejects_duplicate_host_and_unmarked_write(
    tailscale_settings: Settings,
) -> None:
    """[VALIDATION] 歧义Host和缺少同源Origin的写请求不能借Serve身份进入。"""

    async def scenario() -> None:
        await initialize(tailscale_settings)
        app = create_app(tailscale_settings)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 123)), base_url=ORIGIN
            ) as client,
        ):
            duplicate = await client.get(
                "/chat",
                headers=[
                    ("X-Forwarded-For", "100.64.0.12"),
                    ("X-Forwarded-Proto", "https"),
                    ("X-Forwarded-Host", HOST),
                    ("Tailscale-User-Login", LOGIN),
                    ("Sec-Fetch-Site", "none"),
                    ("Host", HOST),
                    ("Host", "untrusted.invalid"),
                ],
            )
            assert duplicate.status_code == 403 and "set-cookie" not in duplicate.headers
            write = await client.post(BASE + "/v1/parameter-sets", json={}, headers=serve_headers())
            assert write.status_code == 403 and write.json()["data"] is None

    asyncio.run(scenario())

    asyncio.run(scenario())


def test_validation_tailscale_serve_requires_pair_and_https_origin(tailscale_settings: Settings) -> None:
    """[VALIDATION] 配置缺身份或未允许HTTPS主机必须拒启，旧本机入口保持可用。"""

    with pytest.raises(ValueError, match="同时配置主机"):
        Settings(_env_file=None, tailscale_serve_host=HOST)
    with pytest.raises(ValueError, match="同时配置主机"):
        Settings(_env_file=None, tailscale_serve_login=LOGIN)
    with pytest.raises(ValueError, match="主机无效"):
        Settings(_env_file=None, tailscale_serve_host="bad", tailscale_serve_login=LOGIN)
    with pytest.raises(ValueError, match="HTTPS主机"):
        create_app(tailscale_settings.model_copy(update={"api_allowed_origins": ("http://127.0.0.1:8788",)}))
    local = tailscale_settings.model_copy(update={"tailscale_serve_host": "", "tailscale_serve_login": ""})
    assert create_app(local) is not None
