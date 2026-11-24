"""只读原生PG到实际worker/SQLite/准备账本；pytest不访问外网。"""

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from sqlalchemy import delete, insert, select

from stock_research.benchmarks import BenchmarkBundle
from stock_research.config import MarketSourceSettings, Settings
from stock_research.data import DAILY_BARS
from stock_research.identity import new_id, now
from stock_research.market_refresh import RefreshExecutionError, refresh_market
from stock_research.market_sync import CLOUD_BARS, CLOUD_METADATA
from stock_research.market_writer import checkpoint as sync_checkpoint
from stock_research.market_writer import local_write_engine
from stock_research.models import Bar
from stock_research.processes import isolated_environment
from stock_research.schema import PREPARATION_ATTEMPTS
from stock_research.schemas.market_refresh import RefreshRequest
from stock_research.source_guard import LOADED_SOURCE_IDENTITY
from stock_research.store import close_engine, database_engine, initialize_schema
from tests.stock_research.test_benchmarks import bundle as bundle
from tests.stock_research.test_market_sync import source_settings
from tests.stock_research.test_premarket_api import WORKFLOW_TOKEN, Clock, intent, seed_intent, settled
from tests.stock_research.test_premarket_api import configured as configured
from tests.stock_research.test_preparations import preparation_settings as preparation_settings

pytestmark = pytest.mark.usefixtures("local_postgres_socket")
AS_OF = datetime(2026, 11, 20, tzinfo=UTC).date()


async def seed_full_source(settings: Settings, bars: tuple[Bar, ...], *, invalid: bool = False) -> None:
    engine = database_engine(settings, test=True)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(CLOUD_METADATA.create_all)
            await connection.execute(delete(CLOUD_BARS))
            await connection.execute(
                insert(CLOUD_BARS),
                [
                    {
                        "symbol": ("US:" if bar.kind == "stock" else "INDEX:US:") + bar.symbol,
                        "adjust": "qfq",
                        "date": bar.session_date,
                        "updated_at": bar.source_revision,
                        **{name: getattr(bar, name) for name in ("open", "high", "low", "volume")},
                        "close": None if invalid and bar.kind == "stock" and bar.session_date == AS_OF else bar.close,
                    }
                    for bar in bars
                ],
            )
    finally:
        await close_engine(engine)


def source_environment(monkeypatch: pytest.MonkeyPatch, source: MarketSourceSettings) -> None:
    for key, value in source.model_dump().items():
        monkeypatch.setenv("PG_" + key.upper(), value.get_secret_value() if key == "password" else str(value or ""))
    monkeypatch.setenv("PG_ENABLED", "true")


def test_real_worker_syncs_without_inheriting_other_identities(
    postgres_settings: Settings, bars: tuple[Bar, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        await seed_full_source(postgres_settings, bars)
        settings = Settings(
            _env_file=None, local_market_path=tmp_path / "new.sqlite3", artifacts_path=tmp_path / "artifacts"
        )
        receipts, children, environments = [], [], []
        original = asyncio.create_subprocess_exec

        async def spawn(*args, **kwargs):
            assert "synthetic-parent-secret" not in " ".join(args)
            environments.append(kwargs["env"])
            child = await original(*args, **kwargs)
            children.append(child)
            return child

        async def record(receipt):
            receipts.append(receipt)

        monkeypatch.setenv("STOCK_RESEARCH_API_TOKEN", "synthetic-parent-secret")
        monkeypatch.setenv("PG_PASSWORD", "synthetic-parent-secret")
        monkeypatch.setenv("SA_INTERNAL_TOKEN", "synthetic-parent-secret")
        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
        updated = await refresh_market(
            settings, AS_OF, timeout=20, checkpoint=record, source=source_settings(postgres_settings)
        )
        assert updated == settings and [item.status for item in receipts] == ["unconfirmed", "unconfirmed", "completed"]
        assert receipts[1].sync.rows == 1280 and receipts[-1].sync.issues == 0
        assert "synthetic-parent-secret" not in receipts[-1].model_dump_json()
        assert children[0].returncode == 0 and not list((settings.artifacts_path / "market-jobs").iterdir())
        assert not any(key.startswith(("PG_", "STOCK_RESEARCH_", "SA_")) for key in environments[0])
        local = local_write_engine(settings.local_market_path)
        try:
            assert await sync_checkpoint(local, receipts[-1].sync.scope) == receipts[-1].sync.checkpoint
            async with local.connect() as connection:
                assert len((await connection.execute(select(DAILY_BARS))).all()) == 1280
        finally:
            await close_engine(local)
        with pytest.raises(ProcessLookupError):
            os.killpg(children[0].pid, 0)

    asyncio.run(scenario())


@pytest.mark.parametrize("invalid", [False, True])
def test_premarket_actual_refresh_is_saved_before_quality_and_run(
    configured: tuple[Settings, Clock],
    postgres_settings: Settings,
    bars: tuple[Bar, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: bool,
) -> None:
    from stock_research.http_api import create_app

    async def scenario() -> None:
        settings, _ = configured
        await seed_full_source(postgres_settings, bars, invalid=invalid)
        source_environment(monkeypatch, source_settings(postgres_settings))
        settings = settings.model_copy(
            update={"premarket_refresh": True, "local_market_path": tmp_path / "fresh.sqlite3"}
        )
        engine = database_engine(settings)
        try:
            await initialize_schema(engine)
            app = create_app(settings)
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://127.0.0.1:8788",
                    headers={"Authorization": "Bearer " + WORKFLOW_TOKEN},
                ) as client,
            ):
                data = intent()
                job_id = await seed_intent(app.state.manager, data)
                response = await client.post("/v1/premarket-jobs", json=data.model_dump(mode="json"))
                assert response.status_code == 202, response.text
                final = await settled(client, job_id)
                refresh = final["preparation"]["market_refresh"]
                assert refresh["status"] == "completed" and refresh["as_of"] == str(AS_OF)
                assert refresh["sync"]["rows"] == (1279 if invalid else 1280)
                assert final["status"] == ("failed_preparation" if invalid else "completed")
                assert (final["run"] is None) is invalid
                async with engine.connect() as connection:
                    stored = await connection.scalar(
                        select(PREPARATION_ATTEMPTS.c.payload).where(
                            PREPARATION_ATTEMPTS.c.request_uuid == job_id,
                        )
                    )
                assert stored["market_refresh"] == refresh
                assert "password" not in json.dumps(refresh) and "benchmark_path" not in refresh
        finally:
            await close_engine(engine)

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["failure", "timeout", "cancel"])
def test_provider_failure_or_stop_preserves_confirmed_sync_and_reaps_process(
    postgres_settings: Settings,
    bars: tuple[Bar, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    async def scenario() -> None:
        await seed_full_source(postgres_settings, bars)
        settings = Settings(
            _env_file=None,
            local_market_path=tmp_path / "market.sqlite3",
            artifacts_path=tmp_path / "artifacts",
            benchmark_path=tmp_path / "enabled.json",
        )
        original = asyncio.create_subprocess_exec
        children, receipts = [], []
        synced = asyncio.Event()
        provider = (
            "def fail(*args): raise RuntimeError('synthetic-provider-secret')\n"
            if kind == "failure"
            else "signal.signal(signal.SIGTERM,signal.SIG_IGN)\ndef fail(*args): time.sleep(60)\n"
        )

        async def spawn(*args, **kwargs):
            bootstrap = (
                "import runpy,signal,time; import stock_research.benchmarks as b\n"
                + provider
                + "b.fetch_benchmarks=fail\nrunpy.run_module('stock_research.market_refresh_worker',run_name='__main__')"
            )
            child = await original(args[0], "-c", bootstrap, **kwargs)
            children.append(child)
            return child

        async def record(receipt):
            receipts.append(receipt)
            if receipt.sync is not None:
                synced.set()

        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
        task = asyncio.create_task(
            refresh_market(settings, AS_OF, timeout=8, checkpoint=record, source=source_settings(postgres_settings))
        )
        await asyncio.wait_for(synced.wait(), 6)
        if kind == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(RefreshExecutionError):
                await task
        final = receipts[-1]
        assert final.sync.rows == 1280 and final.finished_at is not None
        assert (
            final.error_code
            == {"failure": "refresh_benchmark_failed", "timeout": "refresh_timeout", "cancel": "refresh_cancelled"}[
                kind
            ]
        )
        assert "synthetic-provider-secret" not in final.model_dump_json()
        assert children[0].returncode is not None and not list((settings.artifacts_path / "market-jobs").iterdir())
        with pytest.raises(ProcessLookupError):
            os.killpg(children[0].pid, 0)

    asyncio.run(scenario())


def test_successful_explicit_benchmark_is_revalidated_and_used_only_for_this_prepare(
    postgres_settings: Settings,
    bars: tuple[Bar, ...],
    bundle: BenchmarkBundle,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        await seed_full_source(postgres_settings, bars)
        fixture = tmp_path / "supplier.json"
        fixture.write_text(bundle.model_dump_json())
        original = asyncio.create_subprocess_exec
        settings = Settings(
            _env_file=None,
            local_market_path=tmp_path / "market.sqlite3",
            artifacts_path=tmp_path / "artifacts",
            benchmark_path=tmp_path / "old.json",
        )

        async def spawn(*args, **kwargs):
            bootstrap = (
                "import runpy;from pathlib import Path;import stock_research.benchmarks as b\n"
                "def provide(as_of,rows,destination):\n"
                f" raw=Path({str(fixture)!r}).read_text();bundle=b.BenchmarkBundle.model_validate_json(raw)\n"
                " path=destination/(bundle.content_hash+'.json');b.write_once(path,raw)\n"
                " return {'path':str(path.resolve()),'content_hash':bundle.content_hash}\n"
                "b.fetch_benchmarks=provide\nrunpy.run_module('stock_research.market_refresh_worker',run_name='__main__')"
            )
            return await original(args[0], "-c", bootstrap, **kwargs)

        receipts = []

        async def record(receipt):
            receipts.append(receipt)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
        updated = await refresh_market(
            settings, AS_OF, timeout=20, checkpoint=record, source=source_settings(postgres_settings)
        )
        assert updated.benchmark_path == settings.artifacts_path / "benchmarks" / (bundle.content_hash + ".json")
        assert settings.benchmark_path == tmp_path / "old.json"
        assert receipts[-1].benchmark_hash == bundle.content_hash and receipts[-1].status == "completed"
        assert BenchmarkBundle.model_validate_json(updated.benchmark_path.read_text()) == bundle

    asyncio.run(scenario())


def test_actual_parent_sigkill_closes_control_and_stops_worker(
    postgres_settings: Settings,
    bars: tuple[Bar, ...],
    tmp_path: Path,
) -> None:
    from datetime import timedelta

    async def scenario() -> None:
        await seed_full_source(postgres_settings, bars)
        request = RefreshRequest(
            id=new_id(),
            source_hash=LOADED_SOURCE_IDENTITY,
            as_of=AS_OF,
            history_rows=320,
            local_path=tmp_path / "market.sqlite3",
            benchmark_directory=tmp_path / "benchmarks",
            fetch_benchmarks=True,
            deadline=now() + timedelta(seconds=30),
            source=source_settings(postgres_settings),
        )
        child_code = (
            "import runpy,time;import stock_research.benchmarks as b\n"
            "def wait(*args):time.sleep(60)\nb.fetch_benchmarks=wait\n"
            "runpy.run_module('stock_research.market_refresh_worker',run_name='__main__')"
        )
        parent_code = (
            "import subprocess,sys\n"
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r}],stdin=subprocess.PIPE,stdout=subprocess.PIPE,start_new_session=True)\n"
            "p.stdin.write(sys.stdin.buffer.readline());p.stdin.flush()\n"
            "for raw in iter(p.stdout.readline,b''):\n sys.stdout.buffer.write(raw);sys.stdout.buffer.flush()\n"
        )
        parent = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            parent_code,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=tmp_path,
            env={**isolated_environment(), "HOME": str(tmp_path)},
        )
        try:
            wire = request.model_dump(mode="json")
            wire["source"]["password"] = request.source.password.get_secret_value()
            parent.stdin.write((json.dumps(wire) + "\n").encode())
            await parent.stdin.drain()
            started = json.loads(await asyncio.wait_for(parent.stdout.readline(), 5))
            synced = json.loads(await asyncio.wait_for(parent.stdout.readline(), 5))
            assert started["kind"] == "started" and synced["kind"] == "synced" and synced["sync"]["rows"] == 1280
            os.kill(parent.pid, signal.SIGKILL)
            await asyncio.wait_for(parent.wait(), 5)
            async with asyncio.timeout(8):
                while True:
                    try:
                        os.kill(started["pid"], 0)
                    except ProcessLookupError:
                        break
                    await asyncio.sleep(0.05)
            with pytest.raises(ProcessLookupError):
                os.killpg(started["pid"], 0)
        finally:
            if parent.returncode is None:
                parent.kill()
                await parent.wait()
            if parent.stdin:
                parent.stdin.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("kind", ["configuration", "binding", "oversize"])
def test_invalid_configuration_or_protocol_is_rejected_without_adopting_data(
    postgres_settings: Settings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            _env_file=None, local_market_path=tmp_path / "market.sqlite3", artifacts_path=tmp_path / "artifacts"
        )
        source = source_settings(postgres_settings)
        if kind == "configuration":
            source = source.model_copy(update={"enabled": False})
        original = asyncio.create_subprocess_exec
        children, receipts = [], []

        async def spawn(*args, **kwargs):
            bootstrap = (
                "import json,os,sys,time\nr=json.loads(sys.stdin.buffer.readline())\n"
                + (
                    "sys.stdout.write('x'*17000+'\\n')\n"
                    if kind == "oversize"
                    else "sys.stdout.write(json.dumps({'kind':'started','id':r['id'],'source_hash':r['source_hash'],'pid':os.getpid()+1})+'\\n')\n"
                )
                + "sys.stdout.flush();time.sleep(60)"
            )
            child = await original(args[0], "-c", bootstrap, **kwargs)
            children.append(child)
            return child

        async def record(receipt):
            receipts.append(receipt)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
        with pytest.raises(RefreshExecutionError) as raised:
            await refresh_market(settings, AS_OF, timeout=15, checkpoint=record, source=source)
        code = "refresh_configuration_invalid" if kind == "configuration" else "refresh_protocol_invalid"
        assert raised.value.receipt.error_code == code and receipts[-1].error_code == code
        assert receipts[-1].sync is None and not settings.local_market_path.exists()
        assert (not children) is (kind == "configuration")
        for child in children:
            assert child.returncode is not None
            with pytest.raises(ProcessLookupError):
                os.killpg(child.pid, 0)

    asyncio.run(scenario())


@pytest.mark.parametrize("invalid", [False, True])
def test_explicit_cli_reports_transport_and_isolation_without_claiming_quality(
    postgres_settings: Settings,
    bars: tuple[Bar, ...],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid: bool,
) -> None:
    from stock_research.cli import execute, parser

    asyncio.run(seed_full_source(postgres_settings, bars, invalid=invalid))
    source_environment(monkeypatch, source_settings(postgres_settings))
    settings = Settings(
        _env_file=None, local_market_path=tmp_path / "market.sqlite3", artifacts_path=tmp_path / "artifacts"
    )
    result, status = execute(parser().parse_args(["refresh-market", "--as-of", str(AS_OF)]), settings)
    assert not settings.premarket_refresh and status == (2 if invalid else 0)
    assert result["refresh"]["status"] == "completed" and result["quality_checked"] is False
    assert result["refresh"]["sync"]["rows"] == (1279 if invalid else 1280)
    assert result["refresh"]["sync"]["issues"] == int(invalid)
    assert "password" not in json.dumps(result)
