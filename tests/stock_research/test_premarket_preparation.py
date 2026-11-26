"""盘前准备等待必须服从原预算，取消和失败后释放原生PG锁。"""

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import func, select

from stock_research.calendar import session_times
from stock_research.config import Settings
from stock_research.errors import ResearchUnavailable
from stock_research.preparations import premarket_preparation_lock, preparation_page
from stock_research.run_manager import RunManager, prepare_from_market
from stock_research.run_store import RunStore
from stock_research.schema import MARKET_SNAPSHOTS, RESEARCH_RUNS
from stock_research.schemas.premarket import PremarketBinding
from stock_research.schemas.requests import CreateRun
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_premarket_api import Clock, intent, seed_intent
from tests.stock_research.test_premarket_api import configured as configured
from tests.stock_research.test_preparations import preparation_settings as preparation_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")


@pytest.mark.parametrize("stop", [pytest.param("cancel", id="cancel"), pytest.param("deadline", id="deadline")])
def test_error_waiting_preparation_stops_without_snapshot(configured: tuple[Settings, Clock], stop: str) -> None:
    """【异常值】等待中的准备取消或耗尽原期限后落库，不能继续读取行情。"""

    async def scenario() -> None:
        settings, clock = configured
        engine, peer = database_engine(settings), database_engine(settings)
        store = RunStore(engine, settings.workspace)
        target = intent().schedule.scheduled_for.date()
        previous = session_times(target)[2]
        try:
            await initialize_schema(engine)
            event = intent(kind="manual")
            job_id = await seed_intent(RunManager(store, settings), event)
            deadline = clock.at + timedelta(minutes=50)
            binding = PremarketBinding(job_id=job_id, schedule=event.schedule, execution_deadline=deadline)
            if stop == "deadline":
                clock.at = deadline - timedelta(seconds=0.4)
            async with premarket_preparation_lock(
                RunStore(peer, settings.workspace), settings.artifacts_path, previous, enabled=True
            ):
                task = asyncio.create_task(
                    prepare_from_market(store, settings, CreateRun(target_session=target), job_id, premarket=binding)
                )
                async with asyncio.timeout(5):
                    while (await preparation_page(store, 1, 10)).total == 0:
                        await asyncio.sleep(0.01)
                if stop == "cancel":
                    task.cancel()
                with pytest.raises(asyncio.CancelledError if stop == "cancel" else ResearchUnavailable):
                    await asyncio.wait_for(task, 2)

                item = (await preparation_page(store, 1, 10)).items[0]
                assert item.status == ("cancelled" if stop == "cancel" else "failed")
                assert item.error_code == ("cancelled" if stop == "cancel" else "input_unavailable")
                assert item.run_id is None and item.quality is None
                assert not settings.artifacts_path.exists()
                async with engine.connect() as connection:
                    assert [
                        await connection.scalar(
                            select(func.count()).select_from(table).where(table.c.workspace == settings.workspace)
                        )
                        for table in (RESEARCH_RUNS, MARKET_SNAPSHOTS)
                    ] == [0, 0]
            async with (
                asyncio.timeout(2),
                premarket_preparation_lock(store, settings.artifacts_path, previous, enabled=True),
            ):
                pass
        finally:
            await close_engine(engine)
            await close_engine(peer)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "scope", [pytest.param("root", id="different-root"), pytest.param("date", id="different-date")]
)
def test_normal_independent_snapshot_scope_does_not_wait(configured: tuple[Settings, Clock], scope: str) -> None:
    """【正常值】同库其他产物根目录或行情日期的准备不被共享门锁阻塞。"""

    async def scenario() -> None:
        settings, _ = configured
        engine, peer = database_engine(settings), database_engine(settings)
        store = RunStore(engine, settings.workspace)
        previous = session_times(intent().schedule.scheduled_for.date())[2]
        root = settings.artifacts_path
        try:
            async with premarket_preparation_lock(store, root, previous, enabled=True):
                async with (
                    asyncio.timeout(2),
                    premarket_preparation_lock(
                        RunStore(peer, settings.workspace),
                        root / "other" if scope == "root" else root,
                        previous - timedelta(days=1) if scope == "date" else previous,
                        enabled=True,
                    ),
                ):
                    pass
        finally:
            await close_engine(engine)
            await close_engine(peer)

    asyncio.run(scenario())


def test_error_failed_owner_releases_gate_for_new_owner(configured: tuple[Settings, Clock]) -> None:
    """【异常值】真实输入损坏失败后，新拥有者重试可冻结，失败记录保持原样。"""

    async def scenario() -> None:
        settings, clock = configured
        engine, peer = database_engine(settings), database_engine(settings)
        store, other = RunStore(engine, settings.workspace), RunStore(peer, settings.workspace)
        original = settings.universe_path.read_text()
        target = intent().schedule.scheduled_for.date()
        try:
            await initialize_schema(engine)
            event = intent(kind="manual")
            job_id = await seed_intent(RunManager(store, settings), event)
            binding = PremarketBinding(
                job_id=job_id, schedule=event.schedule, execution_deadline=clock.at + timedelta(minutes=50)
            )
            settings.universe_path.write_text("{")
            with pytest.raises(ResearchUnavailable):
                await prepare_from_market(store, settings, CreateRun(target_session=target), job_id, premarket=binding)
            failed = (await preparation_page(store, 1, 10)).items[0]
            settings.universe_path.write_text(original)

            async with asyncio.timeout(5):
                run_id = await prepare_from_market(
                    other, settings, CreateRun(target_session=target), job_id, premarket=binding
                )

            page = await preparation_page(store, 1, 10)
            assert page.total == 2
            ready = next(item for item in page.items if item.status == "ready")
            assert str(ready.run_id) == run_id and ready.quality.passed
            assert next(item for item in page.items if item.id == failed.id) == failed
            assert failed.status == "failed" and failed.error_code == "input_unavailable"
        finally:
            settings.universe_path.write_text(original)
            await close_engine(engine)
            await close_engine(peer)

    asyncio.run(scenario())
