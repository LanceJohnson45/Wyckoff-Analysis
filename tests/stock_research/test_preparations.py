"""准备失败真实落库与独立查询；测试不调用外部行情或模型。"""

import asyncio
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, delete, update

from stock_research.cli import execute, parser
from stock_research.config import Settings
from stock_research.data import DAILY_BARS
from stock_research.errors import ResearchConflict, ResearchIntegrityError, ResearchQualityError
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.preparations import preparation_page, record_preparation
from stock_research.research_commands import run_command
from stock_research.run_store import RunStore
from stock_research.schema import PREPARATION_ATTEMPTS
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_http_api import TOKEN

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
READER = "synthetic-read-identity-with-32-characters"
TARGET = date(2026, 11, 23)


@pytest.fixture
def preparation_settings(
    postgres_settings: Settings, snapshot: Snapshot, market_path: Path, tmp_path: Path
) -> Settings:
    """真实入口使用合成行情、版本文件和专属workspace。"""
    universe, classifications = tmp_path / "universe.json", tmp_path / "classifications.json"
    universe.write_text(snapshot.universe.model_dump_json())
    classifications.write_text(snapshot.classifications.model_dump_json())
    return postgres_settings.model_copy(
        update={
            "workspace": "preparations-" + new_id(),
            "postgres_database": "stock_research_test",
            "artifacts_path": tmp_path / "artifacts",
            "local_market_path": market_path,
            "universe_path": universe,
            "classification_path": classifications,
            "api_token": Settings(_env_file=None, api_token=TOKEN).api_token,
            "api_read_token": Settings(_env_file=None, api_read_token=READER).api_read_token,
        }
    )


def test_error_http_quality_failure_preserves_report_and_all_attempts(preparation_settings: Settings) -> None:
    """【异常值】真实行情跌破覆盖门槛时不建run，失败尝试可分页独立读回。"""

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        headers = {"Authorization": f"Bearer {TOKEN}", "Idempotency-Key": "fixture-preparation-initial"}
        try:
            await initialize_schema(engine)
            app = create_app(preparation_settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:8788", headers=headers
                ) as client,
            ):
                created = await client.post("/v1/research-runs", json={"target_session": str(TARGET)})
                assert created.status_code == 202
                run_id = created.json()["data"]["run_id"]
                async with asyncio.timeout(10):
                    while (await app.state.store.summary(run_id))["status"] in {"pending", "running"}:
                        await asyncio.sleep(0.01)
                original = (await client.get("/v1/reports/latest")).json()["data"]
                source = create_engine("sqlite:///" + str(preparation_settings.local_market_path))
                try:
                    with source.begin() as connection:
                        connection.execute(delete(DAILY_BARS).where(DAILY_BARS.c.kind == "stock"))
                finally:
                    source.dispose()

                body = {"target_session": str(TARGET), "force": True, "parent_run_id": run_id}
                failure_headers = {**headers, "Idempotency-Key": "fixture-preparation-quality-failure"}
                assert (await client.post("/v1/research-runs", json=body, headers=failure_headers)).status_code == 503
                assert (await client.post("/v1/research-runs", json=body, headers=failure_headers)).status_code == 503
                page = (
                    await client.get("/v1/preparations?page_size=1", headers={"Authorization": f"Bearer {READER}"})
                ).json()["data"]
                assert page["total"] == 3
                failed = page["items"][0]
                assert failed["status"] == "failed" and failed["error_code"] == "quality_failed"
                assert failed["phase"] == "snapshot" and failed["run_id"] is None
                assert failed["quality"]["coverage"] == 0 and failed["quality"]["passed"] is False
                assert (await client.get("/v1/reports/latest")).json()["data"] == original
                assert (await client.get("/v1/research-runs")).json()["data"]["total"] == 1
                assert (await client.get("/v1/preparations?page=0")).status_code == 422
                assert (await client.get("/v1/preparations?page_size=101")).status_code == 422
                assert (await client.get("/v1/preparations", headers={"Authorization": ""})).status_code == 401
                assert (
                    await client.post("/v1/preparations", headers={"Authorization": f"Bearer {READER}"})
                ).status_code == 403
                assert (await client.get("/v1/preparations?target_session=2026-11-24")).json()["data"]["total"] == 0
                assert (await client.get("/v1/preparations?page=3&page_size=1")).json()["data"]["items"][0][
                    "status"
                ] == "ready"
            independent = await preparation_page(RunStore(engine, preparation_settings.workspace), 1, 10)
            assert len({item.id for item in independent.items}) == 3
            assert independent.items[0].request_id == independent.items[1].request_id
            assert (await preparation_page(RunStore(engine, "other-" + new_id()), 1, 10)).total == 0
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_cli_missing_snapshot_is_recorded_without_internal_paths(
    preparation_settings: Settings, tmp_path: Path
) -> None:
    """【异常值】实际CLI消费路径失败仍落库，回执不包含文件名或异常内容。"""

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        try:
            await initialize_schema(engine)
            with pytest.raises(OSError):
                await run_command(preparation_settings, tmp_path / "private-secret-snapshot.json", TARGET)

            page = await preparation_page(RunStore(engine, preparation_settings.workspace), 1, 10)
            assert page.total == 1
            item = page.items[0]
            assert item.status == "failed" and item.error_code == "input_unavailable"
            assert item.origin == "cli" and item.request_id is None and item.quality is None
            assert item.phase == "snapshot" and item.run_id is None
            assert str(tmp_path) not in page.model_dump_json() and "private-secret" not in page.model_dump_json()
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_default_cli_snapshot_is_inside_preparation(preparation_settings: Settings) -> None:
    """【异常值】不指定快照的实际CLI入口也在覆盖率失败前提交尝试。"""
    source = create_engine("sqlite:///" + str(preparation_settings.local_market_path))
    try:
        with source.begin() as connection:
            connection.execute(delete(DAILY_BARS).where(DAILY_BARS.c.kind == "stock"))
    finally:
        source.dispose()

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        try:
            await initialize_schema(engine)
            args = parser().parse_args(["run", "--market", "us", "--target-session", str(TARGET), "--preview"])
            with pytest.raises(ResearchQualityError):
                await asyncio.to_thread(execute, args, preparation_settings)

            item = (await preparation_page(RunStore(engine, preparation_settings.workspace), 1, 10)).items[0]
            assert item.origin == "cli" and item.phase == "snapshot" and item.run_id is None
            assert item.error_code == "quality_failed" and item.status == "failed"
            assert item.quality is not None and item.quality.coverage == 0 and not item.quality.passed
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_normal_cli_ready_is_distinct_from_model_completion(
    preparation_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """【正常值】成功CLI在模型执行前确认固定run，终态回执不可覆盖。"""
    path = tmp_path / "snapshot.json"
    path.write_text(snapshot.model_dump_json())

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        try:
            await initialize_schema(engine)
            result = await run_command(preparation_settings, path, TARGET)

            store = RunStore(engine, preparation_settings.workspace)
            item = (await preparation_page(store, 1, 10)).items[0]
            assert item.status == "ready" and str(item.run_id) == result["run_id"] and item.quality.passed
            assert item.phase == "freeze" and item.error_code is None
            async with record_preparation(store, TARGET, "cli") as attempt:
                await attempt.checkpoint("freeze", snapshot.quality)
                await attempt.ready(result["run_id"])
                with pytest.raises(ResearchConflict):
                    await attempt.save(status="failed", run_id=None, error_code="input_unavailable")
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_cancelled_preparation_records_terminal_state(preparation_settings: Settings) -> None:
    """【异常值】取消等待实际落库，不留下仍在运行的假状态。"""

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        store = RunStore(engine, preparation_settings.workspace)
        started = asyncio.Event()

        async def worker() -> None:
            async with record_preparation(store, TARGET, "cli") as attempt:
                await attempt.checkpoint("snapshot")
                started.set()
                await asyncio.Event().wait()

        try:
            await initialize_schema(engine)
            task = asyncio.create_task(worker())
            await asyncio.wait_for(started.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

            item = (await preparation_page(store, 1, 10)).items[0]
            assert item.status == "cancelled" and item.error_code == "cancelled" and item.run_id is None
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def test_error_hard_exit_preserves_unconfirmed_receipt(preparation_settings: Settings) -> None:
    """【异常值】真实子进程硬退出后已提交记录保持未确认，不推断失败或成功。"""

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        try:
            await initialize_schema(engine)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
    script = """import asyncio, os
from datetime import date
from stock_research.config import Settings
from stock_research.preparations import record_preparation
from stock_research.run_store import RunStore
from stock_research.store import database_engine
async def run():
    settings = Settings.model_validate_json(os.environ['PREPARATION_TEST_SETTINGS'])
    context = record_preparation(RunStore(database_engine(settings), settings.workspace), date(2026, 11, 23), 'cli')
    await context.__aenter__()
    os._exit(0)
asyncio.run(run())
"""
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("STOCK_RESEARCH_", "PG_"))}
    environment["PREPARATION_TEST_SETTINGS"] = preparation_settings.model_dump_json()
    subprocess.run([sys.executable, "-c", script], env=environment, capture_output=True, check=True, timeout=15)

    async def readback() -> None:
        engine = database_engine(preparation_settings)
        try:
            item = (await preparation_page(RunStore(engine, preparation_settings.workspace), 1, 10)).items[0]
            assert item.status == "unconfirmed" and item.run_id is None and item.error_code is None
            assert item.quality is None and item.phase == "parameters"
        finally:
            await close_engine(engine)

    asyncio.run(readback())


def test_validation_corrupted_receipt_fails_closed(preparation_settings: Settings) -> None:
    """【参数校验】摘要损坏不能静默变成空历史或可用回执。"""

    async def scenario() -> None:
        engine = database_engine(preparation_settings)
        store = RunStore(engine, preparation_settings.workspace)
        try:
            await initialize_schema(engine)
            with pytest.raises(OSError):
                async with record_preparation(store, TARGET, "cli"):
                    raise OSError("synthetic-internal-path")
            item = (await preparation_page(store, 1, 10)).items[0]
            async with engine.begin() as connection:
                await connection.execute(
                    update(PREPARATION_ATTEMPTS)
                    .where(PREPARATION_ATTEMPTS.c.uuid == str(item.id))
                    .values(content_hash="f" * 64)
                )

            with pytest.raises(ResearchIntegrityError):
                await preparation_page(store, 1, 10)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
