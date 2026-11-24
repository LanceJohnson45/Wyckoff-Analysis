"""操作状态真实临时PG/子进程与只读、隔离、生命周期验证。"""

import asyncio
import json
import os
import signal
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import URL, Column, DateTime, MetaData, String, Table, event, insert, select
from sqlalchemy.schema import CreateSchema

from scripts import stock_research_delivery_status as reader
from stock_research import operations
from stock_research.config import Settings
from stock_research.errors import ResearchUnavailable
from stock_research.http_api import create_app
from stock_research.identity import SHANGHAI, new_id
from stock_research.models import Snapshot
from stock_research.mvp_delivery import call_worker
from stock_research.mvp_settings import MvpSettings
from stock_research.operations import Operations
from stock_research.pipeline import run_preview
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.operations import DeliveryStatus
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
DAY = date(2026, 11, 24)
STAMP = datetime(2026, 11, 24, 21, 30, tzinfo=SHANGHAI)
TOKEN = "synthetic-operations-operator-token-32"
PERSONAL = Table(
    "task_reports",
    MetaData(schema="personal"),
    Column("workspace_id", String, primary_key=True),
    Column("task_id", String, primary_key=True),
    Column("window_key", String, primary_key=True),
    Column("state", String),
    Column("message_ref", String),
    Column("updated_at", DateTime(timezone=True)),
    Column("body", String),
    Column("target", String),
)


@pytest.fixture
def configured(postgres_settings: Settings, tmp_path: Path) -> tuple[Settings, MvpSettings]:
    """只向临时簇的_test库写合成通知账本，不依赖外部服务或模型。"""
    workspace = "operations-" + new_id()
    mvp = MvpSettings(_env_file=None, **{key: tmp_path / key for key in MvpSettings.model_fields})
    source = URL.create(
        "postgresql",
        username=postgres_settings.postgres_user,
        database=postgres_settings.database_url(test=True).database,
        query={
            "host": postgres_settings.postgres_host,
            "port": str(postgres_settings.postgres_port),
            "sslmode": "disable",
        },
    )
    mvp.personal_env_file.write_text(
        "SA_PG_URL=" + source.render_as_string(hide_password=False) + "\nSA_PERSONAL_WORKSPACE=" + workspace + "\n"
    )
    mvp.personal_env_file.chmod(0o600)
    config = tmp_path / "mvp.env"
    config.write_text("\n".join("STOCK_RESEARCH_MVP_" + k.upper() + "=" + str(v) for k, v in mvp.model_dump().items()))
    config.chmod(0o600)
    settings = Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(),
            "postgres_database": "stock_research_test",
            "workspace": workspace,
            "artifacts_path": tmp_path / "artifacts",
            "api_token": TOKEN,
            "api_read_token": "synthetic-operations-reader-token-32",
            "api_workflow_token": "synthetic-operations-workflow-token-32",
            "mvp_status_config_path": config,
            "premarket_source_workspace": workspace,
        },
    )
    return settings, mvp


async def initialize(settings: Settings) -> None:
    engine = database_engine(settings)
    try:
        await initialize_schema(engine)
        async with engine.begin() as connection:
            await connection.execute(CreateSchema("personal", if_not_exists=True))
            await connection.run_sync(PERSONAL.metadata.create_all)
    finally:
        await close_engine(engine)


def notification(settings: Settings, phase: str, state: str, **changes: object) -> dict[str, object]:
    return {
        "workspace_id": settings.workspace,
        "task_id": "stock-research-premarket",
        "window_key": f"us:{DAY}:{phase}",
        "state": state,
        "message_ref": "om_synthetic" if state == "sent" else "",
        "updated_at": STAMP,
        "body": "private body canary",
        "target": "private target canary",
        **changes,
    }


def test_integration_real_worker_reads_only_scoped_native_ledger(configured: tuple[Settings, MvpSettings]) -> None:
    """[INTEGRATION] 同一时间稳定排序，未知/缺回执不冒充成功，其他任务/工作区不泄露。"""
    settings, mvp = configured

    async def scenario() -> None:
        await initialize(settings)
        engine = database_engine(settings)
        try:
            rows = [
                notification(settings, "initial", "sent"),
                notification(settings, "recovery", "unknown"),
                notification(settings, "initial", "sent", workspace_id="another-" + new_id()),
                notification(settings, "initial", "sent", task_id="different-task"),
                notification(settings, "recovery", "sent", window_key="us:2026-11-23:recovery"),
                notification(settings, "initial", "sent", window_key="us:2026-11-25:initial", message_ref=""),
                notification(
                    settings,
                    "initial",
                    "sent",
                    window_key="us:2026-11-24:manual:initial",
                    updated_at=STAMP + timedelta(days=1),
                ),
                notification(
                    settings,
                    "recovery",
                    "sent",
                    window_key="us:2026-11-25:manual:recovery",
                    updated_at=STAMP + timedelta(days=2),
                ),
            ]
            async with engine.begin() as connection:
                await connection.execute(insert(PERSONAL), rows)
            raw = await call_worker(mvp, "status", {"workspace": settings.workspace, "target_session": str(DAY)}, 8)
            value = DeliveryStatus.model_validate(raw)
            assert value.initial.state == "sent" and value.initial.has_native_receipt
            assert value.recovery.state == "unknown" and not value.recovery.has_native_receipt
            assert value.latest_sent.target_session == DAY and value.latest_sent.phase == "initial"
            assert "private" not in json.dumps(raw) and "message_ref" not in json.dumps(raw)
            missing = DeliveryStatus.model_validate(
                await call_worker(mvp, "status", {"workspace": settings.workspace, "target_session": "2026-11-25"}, 8)
            )
            assert missing.initial.state == "unconfirmed" and missing.recovery.state == "not_recorded"
            with pytest.raises(ValueError, match="进程未完成"):
                await call_worker(mvp, "status", {"workspace": "other", "target_session": str(DAY)}, 8)
            async with engine.connect() as connection:
                saved = (
                    (await connection.execute(select(PERSONAL).where(PERSONAL.c.workspace_id == settings.workspace)))
                    .mappings()
                    .all()
                )
            assert len(saved) == 7 and all(row["body"] == "private body canary" for row in saved)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_integration_status_transaction_is_read_only_and_repeatable(
    configured: tuple[Settings, MvpSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 在实际PG读取transaction标志，不用连接mock声称只读。"""
    settings, mvp = configured
    monkeypatch.setenv("SA_PG_URL", "")
    monkeypatch.setenv("SA_PERSONAL_WORKSPACE", "")
    create = reader.create_async_engine
    modes = []

    def observed(*args, **kwargs):
        engine = create(*args, **kwargs)

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def observe(connection, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().startswith("SELECT") and "personal.task_reports" in statement:
                modes.append(
                    (
                        connection.exec_driver_sql("SHOW transaction_read_only").scalar_one(),
                        connection.exec_driver_sql("SHOW transaction_isolation").scalar_one(),
                    )
                )
                assert ".body" not in statement and ".target" not in statement

        return engine

    monkeypatch.setattr(reader, "create_async_engine", observed)

    async def scenario() -> None:
        await initialize(settings)
        result = await reader.read_status(
            {"workspace": settings.workspace, "target_session": str(DAY)}, mvp.personal_env_file
        )
        assert DeliveryStatus.model_validate(result).latest_sent is None
        assert modes == [("on", "repeatable read"), ("on", "repeatable read")]

    asyncio.run(scenario())


def test_http_operations_private_scope_small_run_projection_and_source_failure(
    configured: tuple[Settings, MvpSettings], snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 无69MB快照读取，完整研究/局部/删除及跨workspace边界独立验证。"""
    settings, _mvp = configured

    async def scenario() -> None:
        await initialize(settings)
        engine = database_engine(settings)
        try:
            made = await run_preview(engine, settings, snapshot, analyses_for(snapshot), snapshot.as_of.replace(day=23))
            async with engine.begin() as connection:
                original = dict(
                    (await connection.execute(select(RESEARCH_RUNS).where(RESEARCH_RUNS.c.uuid == made["run_id"])))
                    .mappings()
                    .one()
                )
                for status, deleted, workspace in (
                    ("partial", False, settings.workspace),
                    ("completed", True, settings.workspace),
                    ("completed", False, "other"),
                ):
                    await connection.execute(
                        insert(RESEARCH_RUNS).values(
                            **{
                                **original,
                                "uuid": new_id(),
                                "run_key": new_id(),
                                "status": status,
                                "is_deleted": deleted,
                                "workspace": workspace,
                                "update_time": STAMP + timedelta(days=1),
                            }
                        )
                    )
            monkeypatch.setattr(
                "stock_research.store.load_snapshot",
                lambda *_args: (_ for _ in ()).throw(AssertionError("operations must not read snapshot")),
            )
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
            ):
                path = "/v1/operations?target_session=" + str(DAY)
                assert (await client.get(path)).status_code == 401
                for key in ("api_read_token", "api_workflow_token"):
                    assert (
                        await client.get(
                            path, headers={"Authorization": "Bearer " + getattr(settings, key).get_secret_value()}
                        )
                    ).status_code == 403
                headers = {"Authorization": "Bearer " + TOKEN}
                response = await client.get(path, headers=headers)
                assert response.status_code == 200, response.text
                body = response.json()["data"]
                assert (
                    body["latest_completed"]["run_id"] == made["run_id"]
                    and body["delivery"]["initial"]["state"] == "not_recorded"
                )
                assert len(response.content) < 2000
                assert (await client.get(path + "&workspace=other", headers=headers)).status_code == 422
                assert (await client.get(path + "&target_session=2026-11-23", headers=headers)).status_code == 422
                app.state.operations.expires = 0
                settings.mvp_status_config_path.unlink()
                failed = await client.get(path, headers=headers)
                assert failed.status_code == 503 and failed.json()["data"] is None and "mvp.env" not in failed.text
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("state", ["preparing", "ready", "sending", "retry", "unknown", "failed", "silent"])
def test_validation_native_status_never_becomes_sent(configured: tuple[Settings, MvpSettings], state: str) -> None:
    """[VALIDATION] 有原生引用也必须保留账本明确状态，不能把未知自动解释为成功。"""
    settings, mvp = configured

    async def scenario() -> None:
        await initialize(settings)
        engine = database_engine(settings)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    insert(PERSONAL).values(**notification(settings, "initial", state, message_ref="om_synthetic"))
                )
            value = DeliveryStatus.model_validate(
                await call_worker(mvp, "status", {"workspace": settings.workspace, "target_session": str(DAY)}, 8)
            )
            assert value.initial.state == state and value.latest_sent is None
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def ledger(target: str) -> dict[str, object]:
    return {
        "target_session": target,
        "checked_at": STAMP.isoformat(),
        "initial": {"state": "not_recorded", "ledger_update_time": None, "has_native_receipt": False},
        "recovery": {"state": "unknown", "ledger_update_time": STAMP.isoformat(), "has_native_receipt": False},
        "latest_sent": None,
    }


def test_concurrent_readers_coalesce_cache_ttl_and_bound_target_retention(
    configured: tuple[Settings, MvpSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    """[CONCURRENT] 多GET共用一次读取，换日期和到期重新核对；缓存仅保留一日。"""
    settings, _mvp = configured
    calls, clock = [], [100.0]
    monkeypatch.setattr(operations, "monotonic", lambda: clock[0])

    async def worker(_settings, kind, payload, _timeout):
        calls.append((kind, payload))
        await asyncio.sleep(0)
        return ledger(payload["target_session"])

    monkeypatch.setattr(operations, "call_worker", worker)

    async def scenario() -> None:
        engine = database_engine(settings)
        monitor = Operations(RunStore(engine, settings.workspace), settings)
        try:
            values = await asyncio.gather(*(monitor.delivery(DAY) for _ in range(4)))
            assert len(calls) == 1 and all(value is values[0] for value in values)
            clock[0] += 10
            await monitor.delivery(DAY)
            assert len(calls) == 2
            await monitor.delivery(DAY + timedelta(days=1))
            assert len(calls) == 3 and monitor.target == DAY + timedelta(days=1)
            await monitor.delivery(DAY)
            assert len(calls) == 4 and monitor.target == DAY
        finally:
            await monitor.close()
            await close_engine(engine)

    asyncio.run(scenario())


def test_bad_response_is_bounded_failed_cache_without_reusing_old_success(
    configured: tuple[Settings, MvpSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    """[EXCEPTION] 非协议/错日期回执不成为空或过期成功，失败五秒后可重新核对。"""
    settings, _mvp = configured
    calls, clock = [], [100.0]
    monkeypatch.setattr(operations, "monotonic", lambda: clock[0])

    async def worker(_settings, _kind, payload, _timeout):
        calls.append(payload)
        return ledger(payload["target_session"]) if len(calls) != 2 else ledger("2026-11-01")

    monkeypatch.setattr(operations, "call_worker", worker)

    async def scenario() -> None:
        engine = database_engine(settings)
        monitor = Operations(RunStore(engine, settings.workspace), settings)
        try:
            assert (await monitor.delivery(DAY)).target_session == DAY
            clock[0] += 10
            results = await asyncio.gather(*(monitor.delivery(DAY) for _ in range(3)), return_exceptions=True)
            assert all(isinstance(result, ResearchUnavailable) for result in results) and len(calls) == 2
            assert monitor.result is None
            clock[0] += 5
            assert (await monitor.delivery(DAY)).target_session == DAY and len(calls) == 3
        finally:
            await monitor.close()
            await close_engine(engine)

    asyncio.run(scenario())


def test_shutdown_cancels_and_reaps_real_stopped_status_child_and_rejects_waiter(
    configured: tuple[Settings, MvpSettings], monkeypatch: pytest.MonkeyPatch
) -> None:
    """[INTEGRATION] 强制停止真实拥有的子进程，关闭仍须最终kill并wait，不遗留worker。"""
    settings, _mvp = configured

    async def scenario() -> None:
        engine = database_engine(settings)
        monitor = Operations(RunStore(engine, settings.workspace), settings)
        original, children, started = asyncio.create_subprocess_exec, [], asyncio.Event()

        async def stalled(*_args, **kwargs):
            code = "import sys,time; sys.stdin.buffer.readline(); time.sleep(60)"
            process = await original(sys.executable, "-c", code, **kwargs)
            children.append(process)
            started.set()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", stalled)
        first = asyncio.create_task(monitor.delivery(DAY))
        try:
            await asyncio.wait_for(started.wait(), 5)
            os.killpg(children[0].pid, signal.SIGSTOP)
            waiter = asyncio.create_task(monitor.delivery(DAY))
            await monitor.close()
            results = await asyncio.gather(first, waiter, return_exceptions=True)
            assert isinstance(results[0], asyncio.CancelledError) and isinstance(results[1], ResearchUnavailable)
            assert len(children) == 1 and children[0].returncode is not None and not monitor.readers
            with pytest.raises(ProcessLookupError):
                os.kill(children[0].pid, 0)
        finally:
            if not first.done():
                first.cancel()
                await asyncio.gather(first, return_exceptions=True)
            await close_engine(engine)

    asyncio.run(scenario())


def test_unconfigured_status_stays_distinct_from_empty_or_failed_source(
    configured: tuple[Settings, MvpSettings],
) -> None:
    settings, _mvp = configured

    async def scenario() -> None:
        await initialize(settings)
        config = settings.model_copy(update={"mvp_status_config_path": None})
        app = create_app(config)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788") as client,
        ):
            response = await client.get(
                "/v1/operations?target_session=" + str(DAY), headers={"Authorization": "Bearer " + TOKEN}
            )
            assert response.status_code == 200 and response.json()["data"]["delivery"] is None
            assert response.json()["data"]["latest_completed"] is None

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["nonzero", "oversized", "invalid", "timeout"])
def test_real_status_child_protocol_failure_and_timeout_are_reaped(
    configured: tuple[Settings, MvpSettings], monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """[EXCEPTION] 有JSON但非零退出仍拒收；超时/越界后必须wait到实际退出。"""
    settings, mvp = configured

    async def scenario() -> None:
        original, children = asyncio.create_subprocess_exec, []
        body = json.dumps(ledger(str(DAY)))
        code = "import sys,time; sys.stdin.buffer.readline(); "
        code += {
            "nonzero": f"print({body!r},flush=True); raise SystemExit(3)",
            "oversized": "print('x'*100001,flush=True)",
            "invalid": "print('{',flush=True)",
            "timeout": "time.sleep(60)",
        }[mode]

        async def child(*_args, **kwargs):
            process = await original(sys.executable, "-c", code, **kwargs)
            children.append(process)
            assert not any(key.startswith(("PG_", "SA_", "STOCK_RESEARCH_")) for key in kwargs["env"])
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", child)
        with pytest.raises((ValueError, TimeoutError)):
            await call_worker(
                mvp,
                "status",
                {"workspace": settings.workspace, "target_session": str(DAY)},
                0.1 if mode == "timeout" else 5,
            )
        assert len(children) == 1 and children[0].returncode is not None
        with pytest.raises(ProcessLookupError):
            os.kill(children[0].pid, 0)

    asyncio.run(scenario())
