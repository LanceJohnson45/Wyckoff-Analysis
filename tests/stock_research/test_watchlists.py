"""真实量化输入与原生PG验证状态保留、日来源去重和事务回滚。"""

import asyncio
from datetime import date
from pathlib import Path

import httpx
import pytest

from stock_research.analysis import compute_martin
from stock_research.config import Settings
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.quality import build_snapshot
from stock_research.run_store import RunStore
from stock_research.store import close_engine, database_engine
from stock_research.watch_policy import assess_martin, assess_wyckoff
from stock_research.watch_queries import event_page, watch_page
from stock_research.watchlists import apply_tracking, next_watch_item
from stock_research.wyckoff import WyckoffParameters, compute_wyckoff
from tests.stock_research.test_run_store import seed


def trending_snapshot(snapshot: Snapshot, trending_bars) -> Snapshot:
    return build_snapshot(
        snapshot.universe,
        snapshot.classifications,
        tuple(bar for bar in snapshot.bars if bar.kind == "benchmark") + trending_bars,
        "synthetic-watch-fixture",
        snapshot.as_of,
    )


def test_martin_states_use_computed_structure_and_preserve_failure(snapshot: Snapshot, trending_bars) -> None:
    valid = trending_snapshot(snapshot, trending_bars)
    near = compute_martin(valid, MartinParameters())
    assert assess_martin(near, "AAPL").near
    arguments = dict(
        symbol="AAPL",
        name="Apple",
        strategy="martin",
        selected=True,
        selection_available=True,
        analysis=near,
        run_id=new_id(),
        snapshot=valid,
        target=date(2026, 11, 23),
    )
    first = next_watch_item(None, **arguments)
    assert first.state == "new"
    failed = next_watch_item(
        first, **{**arguments, "run_id": new_id(), "target": date(2026, 11, 24), "selection_available": False}
    )
    assert failed.state == "new" and failed.not_reviewed
    assert failed.state_run_id == first.state_run_id
    recovered = next_watch_item(failed, **{**arguments, "target": date(2026, 11, 24), "selected": False})
    assert recovered.state == "near" and not recovered.not_reviewed
    assert recovered.state_session == date(2026, 11, 24)
    missing = compute_martin(valid, MartinParameters(ema_periods=(9, 21, 200)))
    insufficient = next_watch_item(recovered, **{**arguments, "analysis": missing, "target": date(2026, 11, 25)})
    assert insufficient.state == "near" and insufficient.data_insufficient
    assert insufficient.evidence_refs == recovered.evidence_refs
    invalid = compute_martin(snapshot, MartinParameters())
    ended = next_watch_item(
        recovered, **{**arguments, "analysis": invalid, "selected": False, "target": date(2026, 11, 25)}
    )
    assert ended.state == "invalid" and not ended.data_insufficient


def test_leaving_support_band_does_not_invalidate_intact_martin_structure(snapshot: Snapshot, trending_bars) -> None:
    latest = trending_bars[-1]
    close = latest.close * 1.05
    moved = latest.model_copy(update={"open": close, "high": close + 0.4, "low": close - 0.4, "close": close})
    valid = trending_snapshot(snapshot, (*trending_bars[:-1], moved))
    analysis = compute_martin(valid, MartinParameters())
    assessment = assess_martin(analysis, "AAPL")
    assert assessment.valid and not assessment.near
    item = next_watch_item(
        None,
        symbol="AAPL",
        name="Apple",
        strategy="martin",
        selected=False,
        selection_available=True,
        analysis=analysis,
        run_id=new_id(),
        snapshot=valid,
        target=date(2026, 11, 23),
    )
    assert item.state == "waiting"


def test_wyckoff_tracking_follows_production_layers_and_retains_crash_structure(spring_snapshot: Snapshot) -> None:
    baseline = compute_wyckoff(spring_snapshot, WyckoffParameters())
    assert baseline.pool == ("AAPL",)
    assert assess_wyckoff(baseline, "AAPL").valid
    limited = compute_wyckoff(spring_snapshot, WyckoffParameters(overrides={"l1_min_close_price": 20.0}))
    assert not assess_wyckoff(limited, "AAPL").reviewed
    bars = tuple(
        bar.model_copy(update={"open": 10.5, "close": 10.0})
        if bar.kind == "benchmark" and bar.symbol == "SPY" and bar.session_date == spring_snapshot.as_of
        else bar
        for bar in spring_snapshot.bars
    )
    crashed = build_snapshot(
        spring_snapshot.universe, spring_snapshot.classifications, bars, "synthetic-crash", spring_snapshot.as_of
    )
    analysis = compute_wyckoff(crashed, WyckoffParameters())
    assert analysis.pool == () and analysis.metrics["benchmark"]["regime"] == "CRASH"
    assert assess_wyckoff(analysis, "AAPL").valid


@pytest.mark.usefixtures("local_postgres_socket")
def test_watchlist_pg_atomic_events_day_owner_and_read_api(
    postgres_settings: Settings, snapshot: Snapshot, trending_bars, tmp_path: Path
) -> None:
    """选股模型不在此测试范围；合成已提交选择输入驱动实际跟踪事务。"""

    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={
                "workspace": "watch-" + new_id(),
                "artifacts_path": tmp_path,
                "postgres_database": "stock_research_test",
            }
        )
        engine = database_engine(settings)
        store = RunStore(engine, settings.workspace)
        try:
            run_id, saved, analyses = await seed(store, trending_snapshot(snapshot, trending_bars))
            body = {
                "run_id": run_id,
                "manifest": {"target_session": "2026-11-23"},
                "selections": [{"strategy": "martin", "payload": {"decision": {"items": [{"symbol": "AAPL"}]}}}],
            }

            async def apply(value, inputs=analyses):
                async with engine.begin() as connection:
                    await apply_tracking(connection, store.workspace, value, saved, inputs)

            await asyncio.gather(apply(body), apply(body))
            first = await watch_page(store, 1, 1)
            assert first.total == 1 and first.items[0].state == "new"
            assert (await event_page(store, 1, 100)).total == 1
            other = await store.create_run({"snapshot_id": saved.id, "nonce": new_id()})
            conflicting = {
                **body,
                "run_id": other,
                "selections": [{"strategy": "martin", "payload": {"decision": {"items": []}}}],
            }
            await apply(conflicting)
            assert (await watch_page(store, 1, 1)).items == first.items
            tomorrow = {**body, "run_id": other, "manifest": {"target_session": "2026-11-24"}, "selections": []}
            await apply(tomorrow)
            held = (await watch_page(store, 1, 1)).items[0]
            assert held.state == "new" and held.not_reviewed
            assert held.state_run_id == first.items[0].state_run_id
            recovered = {**tomorrow, "selections": conflicting["selections"]}
            await apply(recovered)
            assert (await watch_page(store, 1, 1)).items[0].state == "near"
            events = await event_page(store, 1, 100, "martin", "AAPL")
            assert {item.kind for item in events.items} == {"state:near", "health:unreviewed", "state:new"}
            await apply(body)
            assert (await watch_page(store, 1, 1)).items[0].target_session == date(2026, 11, 24)
            with pytest.raises(RuntimeError, match="rollback"):
                async with engine.begin() as connection:
                    await apply_tracking(
                        connection,
                        store.workspace,
                        {**recovered, "manifest": {"target_session": "2026-11-25"}},
                        saved,
                        analyses,
                    )
                    raise RuntimeError("rollback")
            assert (await event_page(store, 1, 100)).total == 3
            await close_engine(engine)
            engine = database_engine(settings)
            assert (await watch_page(RunStore(engine, store.workspace), 1, 1)).items[0].state == "near"
            assert (await watch_page(RunStore(engine, "foreign"), 1, 1)).total == 0
            configured = Settings(
                _env_file=None,
                **settings.model_dump(exclude={"api_token"}),
                api_token="synthetic-watch-operator-with-32-characters",
            )
            app = create_app(configured)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer synthetic-watch-operator-with-32-characters"},
                ) as client,
            ):
                page = await client.get("/v1/watchlists?strategy=martin&page_size=1")
                assert page.json()["data"]["items"][0]["state"] == "near"
                history = await client.get("/v1/watchlists/events?symbol=AAPL&page_size=1")
                assert history.json()["data"]["total"] == 3
                assert len(history.json()["data"]["items"]) == 1
                assert (await client.get("/v1/watchlists?page_size=101")).status_code == 422
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
