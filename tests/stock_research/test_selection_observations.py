"""正式初选观测：价格黄金样本、真实PG绑定和CLI文件重启读回。"""

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, event, insert, select, update

from core.trade_friction import round_trip_cost_pct
from stock_research.calendar import latest_complete_session
from stock_research.config import Settings
from stock_research.data import DAILY_BARS, SOURCE_ISSUES, read_market
from stock_research.decisions import SelectionDecision
from stock_research.errors import ResearchIntegrityError, ResearchNotFound
from stock_research.evaluation_outcomes import indexed_outcomes, observation_sessions, price_index
from stock_research.identity import SHANGHAI, digest, new_id
from stock_research.models import QualityIssue, Snapshot
from stock_research.queries import latest_report_id
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_REPORTS, RESEARCH_RUNS, STRATEGY_SELECTIONS
from stock_research.schemas.selection_observations import SelectionObservationInput, SelectionObservationReport
from stock_research.selection_observation_cli import observation_report, observe_selections_command
from stock_research.selection_scope import forward_reference, selection_scope
from stock_research.store import close_engine, database_engine, initialize_schema, save_snapshot
from tests.stock_research.test_evaluation import daily


@pytest.mark.parametrize(
    ("target", "committed", "expected"),
    [
        ("2026-11-23", "2026-11-23T09:29:59-05:00", "2026-11-23"),
        ("2026-11-23", "2026-11-23T09:30:00-05:00", "2026-11-24"),
        ("2026-11-23", "2026-11-24T10:00:00-05:00", "2026-11-25"),
        ("2026-11-23", "2026-11-21T14:00:00-05:00", "2026-11-23"),
        ("2026-11-25", "2026-11-25T09:31:00-05:00", "2026-11-27"),
        ("2026-11-27", "2026-11-27T12:59:59-05:00", "2026-11-30"),
        ("2026-03-06", "2026-03-06T09:31:00-05:00", "2026-03-09"),
        ("2026-03-09", "2026-03-09T09:29:59-04:00", "2026-03-09"),
    ],
    ids=["before-open", "exact-open", "late-next-day", "weekend", "holiday", "half-day", "dst-weekend", "dst-open"],
)
def test_unit_reference_is_strictly_after_actual_selection(target: str, committed: str, expected: str) -> None:
    """[UNIT] 已发生的开盘不能倒填为模型选择后的参考价。"""
    assert forward_reference(date.fromisoformat(target), datetime.fromisoformat(committed)) == date.fromisoformat(
        expected
    )


def test_unit_reference_rejects_naive_or_non_session_and_uses_half_day_close() -> None:
    """[UNIT] 收盘完整性沿正式日历，包括感恩节后的半日市。"""
    with pytest.raises(ValueError, match="时区"):
        forward_reference(date(2026, 11, 23), datetime(2026, 11, 23, 8))
    with pytest.raises(ValueError, match="交易日"):
        forward_reference(date(2026, 11, 26), datetime(2026, 11, 25, 20, tzinfo=SHANGHAI))
    assert latest_complete_session(datetime.fromisoformat("2026-11-27T12:59:59-05:00")) == date(2026, 11, 25)
    assert latest_complete_session(datetime.fromisoformat("2026-11-27T13:00:00-05:00")) == date(2026, 11, 27)


def test_unit_forward_open_price_golden_and_pending_keep_old_close_contract() -> None:
    """[UNIT] 固定开盘100/200，收盘110/210；当日高低纳入开盘后的MFE/MAE。"""
    day = date(2026, 11, 23)
    bars = (
        daily("AAPL", day, 110).model_copy(update={"open": 100, "high": 120, "low": 90}),
        daily("SPY", day, 210).model_copy(update={"open": 200, "high": 220, "low": 190}),
    )
    result = indexed_outcomes("AAPL", day, day, price_index(bars), reference="forward_open")
    first = result[0]
    cost = round_trip_cost_pct(code="AAPL.US")
    assert first.target_session == day and first.gross_return_pct == pytest.approx(10)
    assert first.net_return_pct == pytest.approx(10 - cost)
    assert first.benchmark_return_pct == pytest.approx(5) and first.net_excess_pct == pytest.approx(5 - cost)
    assert first.mfe_pct == pytest.approx(20) and first.mae_pct == pytest.approx(-10)
    assert all(row.status == "pending" and row.net_return_pct is None for row in result[1:])
    assert (
        indexed_outcomes("AAPL", day, date(2026, 11, 20), price_index(bars), reference="forward_open")[0].status
        == "pending"
    )
    with pytest.raises(ValueError, match="晚于"):
        indexed_outcomes("AAPL", day, date(2026, 11, 20), price_index(bars))


@pytest.mark.parametrize("missing", ["entry", "interior", "benchmark"])
def test_unit_forward_open_five_windows_preserve_missing_denominator(missing: str) -> None:
    """[UNIT] 五窗按XNYS连续日，不向后跳过缺行或错用股票身份的SPY。"""
    days = observation_sessions(date(2026, 10, 19))[:20]
    skip = {"entry": ("AAPL", days[0]), "interior": ("AAPL", days[1]), "benchmark": ("SPY", days[1])}[missing]
    bars = tuple(
        daily(code, day, 100 + index)
        for code in ("AAPL", "SPY")
        for index, day in enumerate(days)
        if (code, day) != skip
    )
    result = indexed_outcomes("AAPL", days[0], days[-1], price_index(bars), reference="forward_open")
    assert [row.target_session for row in result] == [days[0], days[2], days[4], days[9], days[19]]
    assert result[1].status == "missing_data" and result[1].missing_sessions
    assert all(row.net_return_pct is None for row in result[1:])


def test_integration_source_scope_filters_before_row_limit(market_path: Path) -> None:
    """[INTEGRATION] 同一来源有全池数据，SQL只取固定股票/日期/复权。"""
    day = date(2026, 11, 20)
    _, bars, issues = read_market(
        market_path, date(2020, 1, 1), day, max_rows=1, instruments=(("stock", "AAPL"),), session_dates=(day,)
    )
    assert len(bars) == 1 and bars[0].symbol == "AAPL" and not issues
    assert read_market(market_path, day, day, max_rows=1, instruments=(), session_dates=(day,))[1:] == ((), ())
    engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(market_path))
    try:
        with engine.begin() as connection:
            original = (
                connection.execute(select(DAILY_BARS).where(DAILY_BARS.c.symbol == "AAPL", DAILY_BARS.c.date == day))
                .mappings()
                .one()
            )
            connection.execute(insert(DAILY_BARS).values(**{**dict(original), "adjust": "raw"}))
            connection.execute(
                insert(SOURCE_ISSUES).values(
                    market="us", kind="stock", symbol="AAPL", date=day, reason="fixture", cloud_updated_at="fixture"
                )
            )
        _, _, issues = read_market(
            market_path, day, day, max_rows=1, instruments=(("stock", "AAPL"),), session_dates=(day,)
        )
        assert len(issues) == 1 and issues[0].reason == "source_quarantine:2026-11-20"
    finally:
        engine.dispose()


@pytest.fixture
def selection_settings(postgres_settings: Settings, market_path: Path, tmp_path: Path) -> Settings:
    """只指向本fixture原生临时PG与合成行情，CLI不读取根配置。"""
    return Settings(
        _env_file=None,
        **{
            **postgres_settings.model_dump(),
            "workspace": "selection-" + new_id(),
            "postgres_database": "stock_research_test",
            "local_market_path": market_path,
            "artifacts_path": tmp_path / "artifacts",
        },
    )


async def add_selection(store: RunStore, run_id: str, strategy: str, *, empty: bool = False) -> str:
    """合成合法结构的已提交初选，只验证持久读取，不伪装模型执行。"""
    stage = await store.ensure_stage(run_id, strategy, "select")
    point = {"text": "synthetic fixture", "evidence_refs": [strategy + "/AAPL/price"]}
    item = {
        "symbol": "AAPL",
        "rationale": point,
        "risks": [point],
        "confirmation_conditions": [point],
        "claims": [{"ref": strategy + "/AAPL/price", "value": 100}],
    }
    decision = SelectionDecision(
        strategy=strategy, context_hash="a" * 64, items=() if empty else (item,), empty_reason=point if empty else None
    )
    body = {
        "decision": decision.model_dump(mode="json"),
        "context_hash": decision.context_hash,
        "source": "empty_pool" if empty else "model",
        "receipt_path": "synthetic-fixture",
    }
    identifier = new_id()
    async with store.engine.begin() as connection:
        await connection.execute(
            insert(STRATEGY_SELECTIONS).values(
                uuid=identifier,
                workspace=store.workspace,
                run_uuid=run_id,
                stage_uuid=stage,
                strategy=strategy,
                payload=body,
                content_hash=digest(body),
                create_time=datetime(2026, 11, 23, 21, tzinfo=SHANGHAI),
            )
        )
    return identifier


async def seed_report(settings: Settings, snapshot: Snapshot) -> tuple[str, str]:
    """不调用量化或Harness；报告走实际追加发布路径。"""
    engine = database_engine(settings)
    try:
        await initialize_schema(engine)
        saved = await save_snapshot(engine, settings.workspace, snapshot)
        store = RunStore(engine, settings.workspace)
        run = await store.create_run(
            {
                "snapshot_id": saved.id,
                "snapshot_hash": saved.content_hash,
                "target_session": "2026-11-23",
                "preview": True,
                "factor_results": {},
            }
        )
        await add_selection(store, run, "martin")
        await store.publish(run, "partial")
        return run, await latest_report_id(store)
    finally:
        await close_engine(engine)


async def stored_records(settings: Settings) -> dict[str, object]:
    """独立连接核对CLI不改任何研究、报告或初选行。"""
    engine = database_engine(settings)
    try:
        async with engine.connect() as connection:
            return {
                table.name: [
                    dict(row)
                    for row in (
                        await connection.execute(
                            select(table).where(table.c.workspace == settings.workspace).order_by(table.c.uuid)
                        )
                    ).mappings()
                ]
                for table in (RESEARCH_RUNS, RESEARCH_REPORTS, STRATEGY_SELECTIONS)
            }
    finally:
        await close_engine(engine)


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_fixed_report_readonly_identity_and_later_selection_not_added(
    selection_settings: Settings, snapshot: Snapshot
) -> None:
    """[INTEGRATION] 旧报告只读原初选，新报告双重归属及空选均保留。"""

    async def scenario() -> None:
        run, report = await seed_report(selection_settings, snapshot)
        engine = database_engine(selection_settings)
        store = RunStore(engine, selection_settings.workspace)
        modes = []

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def observe(connection, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().startswith("SELECT") and "stock_research." in statement:
                assert "market_snapshots" not in statement
                modes.append(
                    (
                        connection.exec_driver_sql("SHOW transaction_read_only").scalar_one(),
                        connection.exec_driver_sql("SHOW transaction_isolation").scalar_one(),
                    )
                )

        try:
            first = await selection_scope(store, report)
            assert len(first.selections) == 1 and first.selections[0].symbols == ("AAPL",)
            assert modes and set(modes) == {("on", "repeatable read")}
            event.remove(engine.sync_engine, "before_cursor_execute", observe)
            before = await store.row(RESEARCH_REPORTS, report)
            await add_selection(store, run, "wyckoff")
            await store.publish(run, "completed")
            assert await selection_scope(store, report) == first
            latest = await selection_scope(store, await latest_report_id(store))
            assert [row.symbols for row in latest.selections] == [("AAPL",), ("AAPL",)]
            assert await store.row(RESEARCH_REPORTS, report) == before
            with pytest.raises(ResearchNotFound):
                await selection_scope(RunStore(engine, "other-workspace"), report)
            row = await store.row(STRATEGY_SELECTIONS, str(first.selections[0].selection_id))
            async with engine.begin() as connection:
                await connection.execute(
                    update(STRATEGY_SELECTIONS)
                    .where(STRATEGY_SELECTIONS.c.uuid == row["uuid"])
                    .values(content_hash="0" * 64)
                )
            with pytest.raises(ResearchIntegrityError, match="摘要"):
                await selection_scope(store, report)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


def invoke(settings: Settings, report: str, cwd: Path, cutoff: str = "2026-11-24") -> subprocess.CompletedProcess:
    """实际CLI子进程只接临时库/文件配置，模型和Personal身份不传入。"""
    environment = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
    }
    for key in (
        "postgres_host",
        "postgres_port",
        "postgres_database",
        "postgres_user",
        "workspace",
        "local_market_path",
        "artifacts_path",
    ):
        environment["STOCK_RESEARCH_" + key.upper()] = str(getattr(settings, key))
    return subprocess.run(
        [sys.executable, "-m", "stock_research.cli", "observe-selections", "--report-id", report, "--cutoff", cutoff],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_actual_cli_restart_reuse_revision_and_corrupt_rejection(
    selection_settings: Settings, snapshot: Snapshot, tmp_path: Path
) -> None:
    """[INTEGRATION] 完整文件独立重读，修订另起产物，损坏不覆盖或新写研究。"""
    _, report = asyncio.run(seed_report(selection_settings, snapshot))
    before = asyncio.run(stored_records(selection_settings))
    engine = create_engine("sqlite://", creator=lambda: sqlite3.connect(selection_settings.local_market_path))
    try:
        with engine.begin() as connection:
            for symbol, kind, price in (("AAPL", "stock", 100), ("SPY", "benchmark", 200)):
                connection.execute(
                    insert(DAILY_BARS).values(
                        market="us",
                        kind=kind,
                        symbol=symbol,
                        adjust="qfq",
                        date=date(2026, 11, 23),
                        open=price,
                        high=price + 20,
                        low=price - 10,
                        close=price + 10,
                        volume=1000,
                        cloud_updated_at="2026-11-24T08:00:00+08:00",
                        imported_at="2026-11-24T08:00:00+08:00",
                    )
                )
        first = invoke(selection_settings, report, tmp_path)
        assert first.returncode == 0, first.stdout + first.stderr
        result = json.loads(first.stdout)
        path = Path(result["path"])
        frozen = path.read_bytes()
        artifact = SelectionObservationReport.model_validate_json(frozen)
        assert artifact.samples[0].windows[0].gross_return_pct == pytest.approx(10)
        assert result["missing_strategies"] == ["wyckoff"] and result["manual_review"] == "pending"
        repeat = invoke(selection_settings, report, tmp_path)
        assert repeat.returncode == 0 and json.loads(repeat.stdout) == result
        assert path.read_bytes() == frozen
        with engine.begin() as connection:
            connection.execute(
                update(DAILY_BARS)
                .where(DAILY_BARS.c.symbol == "AAPL", DAILY_BARS.c.date == date(2026, 11, 23))
                .values(close=109)
            )
        changed = invoke(selection_settings, report, tmp_path)
        assert changed.returncode == 0 and json.loads(changed.stdout)["path"] != result["path"]
        assert path.read_bytes() == frozen
        other = Path(json.loads(changed.stdout)["path"])
        other.write_text("{}")
        rejected = invoke(selection_settings, report, tmp_path)
        assert rejected.returncode == 1 and other.read_text() == "{}"
        assert "Traceback" not in rejected.stderr
        assert invoke(selection_settings, "not-a-uuid", tmp_path).returncode == 1
        assert invoke(selection_settings, report, tmp_path, "2039-01-03").returncode == 1
        assert asyncio.run(stored_records(selection_settings)) == before
    finally:
        engine.dispose()


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_empty_and_quarantined_initial_selections_are_not_fake_success(
    selection_settings: Settings, snapshot: Snapshot
) -> None:
    """[INTEGRATION] 明确空选不产生样本；来源隔离不能被残留有效行掩盖。"""

    async def scenario() -> None:
        run, report = await seed_report(selection_settings, snapshot)
        engine = database_engine(selection_settings)
        try:
            store = RunStore(engine, selection_settings.workspace)
            await add_selection(store, run, "wyckoff", empty=True)
            await store.publish(run, "completed")
            scope = await selection_scope(store, await latest_report_id(store))
            empty = next(row for row in scope.selections if row.strategy == "wyckoff")
            assert empty.source == "empty_pool" and not empty.symbols
            data = await observe_selections_command(selection_settings, report, date(2026, 11, 24))
            inputs = SelectionObservationInput.model_validate_json(
                Path(data["path"]).with_name("input.json").read_text()
            )
            first = scope.selections[0].reference_session
            point = daily("AAPL", first, 110)
            spy = daily("SPY", first, 200)
            altered = inputs.model_copy(
                update={
                    "scope": scope,
                    "bars": (point, spy),
                    "issues": (QualityIssue(symbol="AAPL", kind="stock", reason="source_quarantine:2026-11-23"),),
                }
            )
            result = observation_report(altered)
            assert len(result.samples) == 1 and result.samples[0].windows[0].status == "missing_data"
            assert result.samples[0].windows[0].missing_sessions == ("stock/AAPL/2026-11-23",)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
@pytest.mark.parametrize("damage", ["target", "run", "time", "payload", "deleted"])
def test_validation_report_selection_binding_rejects_inconsistent_originals(
    selection_settings: Settings, snapshot: Snapshot, damage: str
) -> None:
    """[VALIDATION] 即使重新算过摘要，身份/载荷/时间矛盾仍不能混入观测。"""

    async def scenario() -> None:
        run, report = await seed_report(selection_settings, snapshot)
        engine = database_engine(selection_settings)
        store = RunStore(engine, selection_settings.workspace)
        try:
            selected = (await selection_scope(store, report)).selections[0]
            row = await store.row(RESEARCH_REPORTS, report)
            body = row["payload"]
            async with engine.begin() as connection:
                if damage == "target":
                    body["manifest"]["target_session"] = "2026-11-24"
                elif damage == "run":
                    body["run_id"] = new_id()
                elif damage == "payload":
                    body["selections"][0]["payload"]["source"] = "empty_pool"
                elif damage == "time":
                    await connection.execute(
                        update(STRATEGY_SELECTIONS)
                        .where(STRATEGY_SELECTIONS.c.uuid == str(selected.selection_id))
                        .values(create_time=row["create_time"].replace(year=2030))
                    )
                else:
                    await connection.execute(
                        update(STRATEGY_SELECTIONS)
                        .where(STRATEGY_SELECTIONS.c.uuid == str(selected.selection_id))
                        .values(is_deleted=True)
                    )
                if damage in {"target", "run", "payload"}:
                    await connection.execute(
                        update(RESEARCH_REPORTS)
                        .where(RESEARCH_REPORTS.c.uuid == report)
                        .values(payload=body, content_hash=digest(body))
                    )
            with pytest.raises(ResearchIntegrityError):
                await selection_scope(store, report)
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.usefixtures("local_postgres_socket")
def test_integration_no_initial_selections_retains_missing_strategies(
    selection_settings: Settings, snapshot: Snapshot
) -> None:
    """[INTEGRATION] 无已提交初选的固定修订不补当前名单或伪造空池成功。"""

    async def scenario() -> None:
        run, _ = await seed_report(selection_settings, snapshot)
        engine = database_engine(selection_settings)
        store = RunStore(engine, selection_settings.workspace)
        try:
            manifest = (await store.row(RESEARCH_RUNS, run))["payload"]
            new_run = await store.create_run(manifest, run_key="no-selections-" + new_id())
            await store.publish(new_run, "partial")
            report = await latest_report_id(store, run_id=new_run)
            result = await observe_selections_command(selection_settings, report, date(2026, 11, 24))
            artifact = SelectionObservationReport.model_validate_json(Path(result["path"]).read_text())
            assert artifact.samples == () and artifact.scope.selections == ()
            assert result["selection_count"] == result["observation_count"] == 0
            assert result["missing_strategies"] == ["martin", "wyckoff"]
        finally:
            await close_engine(engine)

    asyncio.run(scenario())
