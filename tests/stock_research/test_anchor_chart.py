"""显示曲线沿用生产锚点，确认之前不显示，也不从请求窗口重置累计。"""

import asyncio
from datetime import timedelta

import httpx
import pytest
from pydantic import SecretStr

from stock_research.analysis import compute_martin
from stock_research.anchor_chart import anchor_directory, anchor_prices
from stock_research.config import Settings
from stock_research.errors import ResearchIntegrityError
from stock_research.http_api import create_app
from stock_research.identity import new_id
from stock_research.models import Bar, Snapshot
from stock_research.parameters import MartinParameters
from stock_research.pipeline import run_preview
from stock_research.query_client import QueryError, ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.schemas.anchor_chart import AnchorDefinition
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_run_store import analyses_for


def test_projection_matches_original_anchors_and_history_cutoff(snapshot: Snapshot) -> None:
    analysis = compute_martin(snapshot, MartinParameters())
    bars = tuple(row for row in snapshot.bars if row.kind == "stock")
    directory = anchor_directory(analysis, "AAPL", bars)
    assert directory
    assert len(directory) == len(analysis.observations[0].anchors)
    anchor = directory[0]
    full, short = anchor_prices(bars, anchor, 320), anchor_prices(bars, anchor, 2)
    assert short == full[-2:]
    assert full[0].session_date == anchor.confirmed_date > anchor.date
    assert full[-1].value == pytest.approx(anchor.daily_estimate)
    assert anchor.ref_prefix == "martin/AAPL/anchors/0"
    damaged = anchor.model_copy(update={"daily_estimate": anchor.daily_estimate + 1})
    with pytest.raises(ResearchIntegrityError, match="不一致"):
        anchor_prices(bars, damaged, 320)
    raw = {**analysis.observations[0].anchors[0], "confirmed_date": str(snapshot.as_of + timedelta(days=1))}
    observation = analysis.observations[0].model_copy(update={"anchors": (raw,)})
    damaged_analysis = analysis.model_copy(update={"observations": (observation,)})
    with pytest.raises(ResearchIntegrityError, match="确认日期"):
        anchor_directory(damaged_analysis, "AAPL", bars)


def test_projection_volume_accumulation_and_no_anchor_reidentification(bars: tuple[Bar, ...]) -> None:
    source = tuple(row for row in bars if row.kind == "stock")[-3:]
    sample = tuple(
        row.model_copy(update={"high": value, "low": value, "close": value, "volume": volume})
        for row, value, volume in zip(source, (10, 20, 40), (0, 2, 1))
    )
    anchor = AnchorDefinition(
        index=0,
        kind="breakout",
        date=sample[0].session_date,
        confirmed_date=sample[0].session_date,
        daily_estimate=80 / 3,
        hard_filter_enabled=False,
        ref_prefix="martin/AAPL/anchors/0",
    )
    points = anchor_prices(sample, anchor, 320)
    assert [row.value for row in points] == [None, 20, pytest.approx(80 / 3)]
    assert anchor_prices(sample, anchor, 1) == points[-1:]
    empty = tuple(row.model_copy(update={"volume": 0}) for row in sample)
    assert all(
        row.value is None for row in anchor_prices(empty, anchor.model_copy(update={"daily_estimate": None}), 320)
    )


@pytest.mark.usefixtures("local_postgres_socket")
def test_fixed_report_anchor_http_and_reader(postgres_settings: Settings, snapshot: Snapshot, tmp_path) -> None:
    async def scenario() -> None:
        settings = postgres_settings.model_copy(
            update={
                "workspace": "anchors-" + new_id(),
                "postgres_database": "stock_research_test",
                "artifacts_path": tmp_path,
                "api_token": SecretStr("synthetic-operator-token-with-32-characters"),
                "api_read_token": SecretStr("synthetic-reader-token-with-32-characters"),
            }
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            analyses = analyses_for(snapshot)
            await run_preview(engine, settings, snapshot, analyses, snapshot.as_of.replace(day=23))
            app = create_app(settings)
            transport = httpx.ASGITransport(app=app)
            reader = ResearchReader(QuerySettings(_env_file=None, token=settings.api_read_token), transport=transport)
            async with app.router.lifespan_context(app):
                report = await reader.latest_report()
                chart = await reader.anchor_chart(report.report_id, "AAPL", 2, 0)
                assert chart.report_id == report.report_id and chart.snapshot_hash == snapshot.content_hash
                assert chart.selected_index == 0 and len(chart.points) == 2
                assert chart.points[-1].value == pytest.approx(chart.anchors[0].daily_estimate)
                assert (await reader.latest_report()).content_hash == report.content_hash
                default = await reader.anchor_chart(report.report_id, "AAPL")
                assert default.anchors[default.selected_index].confirmed_date == max(
                    row.confirmed_date for row in chart.anchors
                )
                for symbol, index in (("ZZZZ", None), ("AAPL", 99999), ("../env", None), ("AAPL", True)):
                    with pytest.raises(QueryError):
                        await reader.anchor_chart(report.report_id, symbol, index=index)
                foreign_app = create_app(settings.model_copy(update={"workspace": "foreign"}))
                foreign = ResearchReader(
                    QuerySettings(_env_file=None, token=settings.api_read_token),
                    transport=httpx.ASGITransport(app=foreign_app),
                )
                async with foreign_app.router.lifespan_context(foreign_app):
                    with pytest.raises(QueryError):
                        await foreign.anchor_chart(report.report_id, "AAPL")
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
