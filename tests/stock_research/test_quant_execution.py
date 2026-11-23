"""实际原策略子进程与退出回收；测试不使用行情网络或模型。"""

import asyncio
import os
import sys
from pathlib import Path

import pytest

from stock_research.config import Settings
from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters
from stock_research.quant_compute import compute_analysis
from stock_research.quant_execution import QuantExecutionError, analyze_isolated, execute_quant, quant_environment
from stock_research.schemas.quant_execution import QuantRequest, QuantStarted
from stock_research.source_guard import LOADED_SOURCE_IDENTITY
from stock_research.wyckoff import WyckoffParameters


def sample_settings(tmp_path: Path, snapshot: Snapshot) -> tuple[Settings, Path]:
    path = tmp_path / "snapshot.json"
    path.write_text(snapshot.model_dump_json())
    martin, wyckoff = tmp_path / "martin.json", tmp_path / "wyckoff.json"
    martin.write_text(MartinParameters().model_dump_json())
    wyckoff.write_text(WyckoffParameters().model_dump_json())
    return Settings(
        _env_file=None, artifacts_path=tmp_path / "artifacts", parameters_path=martin, wyckoff_parameters_path=wyckoff
    ), path


def test_actual_worker_two_strategies_and_cache_reuse(
    snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        settings, path = sample_settings(tmp_path, snapshot)
        original, children, environments, working_dirs = asyncio.create_subprocess_exec, [], [], []
        audits = []

        async def observe(*args, **kwargs):
            audit = tmp_path / f"opens-{len(audits)}.json"
            audits.append(audit)
            bootstrap = (
                "import sys,runpy,json,atexit; from pathlib import Path; opened=[]; "
                "sys.addaudithook(lambda event,args: opened.append(str(args[0])) if event=='open' and isinstance(args[0],(str,bytes)) else None); "
                f"atexit.register(lambda: Path({str(audit)!r}).write_text(json.dumps(opened))); "
                "runpy.run_module('stock_research.quant_worker',run_name='__main__')"
            )
            process = await original(args[0], "-c", bootstrap, **kwargs)
            children.append(process)
            environments.append(kwargs["env"])
            working_dirs.append(kwargs["cwd"])
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", observe)
        monkeypatch.setenv("STOCK_RESEARCH_API_TOKEN", "synthetic-secret-not-for-workers")
        monkeypatch.setenv("PG_PASSWORD", "synthetic-market-secret")
        for strategy, parameters in (("martin", MartinParameters()), ("wyckoff", WyckoffParameters())):
            expected = compute_analysis(snapshot, parameters)
            actual = await analyze_isolated(settings, path, snapshot, strategy)
            assert actual.model_dump(exclude={"create_time", "content_hash"}) == expected.model_dump(
                exclude={"create_time", "content_hash"}
            )
            assert (await analyze_isolated(settings, path, snapshot, strategy)).content_hash == actual.content_hash
        assert len(children) == 2 and all(child.returncode == 0 for child in children)
        assert all(not (set(env) & {"PG_PASSWORD", "STOCK_RESEARCH_API_TOKEN", "HOME"}) for env in environments)
        assert all(not path.exists() for path in working_dirs)
        import json

        for audit in audits:
            opened = json.loads(audit.read_text())
            assert not any(Path(path).name.startswith(".env") or "/.dsh/" in path for path in opened)
        cache = next((settings.artifacts_path / "analyses").glob("martin-*.json"))
        cache.write_text("{}")
        with pytest.raises(ValueError):
            await analyze_isolated(settings, path, snapshot, "martin")
        assert len(children) == 2

    asyncio.run(scenario())


def test_actual_timeout_and_cancellation_reap_worker(
    snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        settings, path = sample_settings(tmp_path, snapshot)
        original, children, started = asyncio.create_subprocess_exec, [], asyncio.Event()

        async def observe(*args, **kwargs):
            process = await original(*args, **kwargs)
            children.append(process)
            started.set()
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", observe)
        with pytest.raises(QuantExecutionError) as error:
            await analyze_isolated(settings, path, snapshot, "martin", timeout=0.02)
        assert error.value.code == "quant_timeout"
        assert children and children[-1].returncode is not None
        started.clear()
        task = asyncio.create_task(analyze_isolated(settings, path, snapshot, "martin"))
        await asyncio.wait_for(started.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        assert children[-1].returncode is not None
        for child in children:
            with pytest.raises(ProcessLookupError):
                os.kill(child.pid, 0)
        assert not list(settings.artifacts_path.rglob("martin-*.json"))

    asyncio.run(scenario())


def test_real_worker_control_eof_and_wrong_binding(snapshot: Snapshot, tmp_path: Path) -> None:
    async def scenario() -> None:
        _settings, path = sample_settings(tmp_path, snapshot)
        request = QuantRequest(
            strategy="wyckoff",
            snapshot_path=path.resolve(),
            snapshot_id=snapshot.id,
            snapshot_hash=snapshot.content_hash,
            source_hash=LOADED_SOURCE_IDENTITY,
            output_path=(tmp_path / "result.json").resolve(),
            parameters=WyckoffParameters(),
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "stock_research.quant_worker",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=tmp_path,
            env=quant_environment(),
            start_new_session=True,
        )
        try:
            process.stdin.write((request.model_dump_json() + "\n").encode())
            await process.stdin.drain()
            ready = QuantStarted.model_validate_json(await asyncio.wait_for(process.stdout.readline(), 10))
            assert ready.pid == process.pid and ready.source_hash == LOADED_SOURCE_IDENTITY
            process.stdin.close()
            await asyncio.wait_for(process.wait(), 5)
            assert process.returncode == 75
            assert not request.output_path.exists()
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), 3)
        for change, code in (
            ({"source_hash": "0" * 64}, "quant_source_changed"),
            ({"snapshot_hash": "0" * 64}, "quant_calculation_failed"),
        ):
            with pytest.raises(QuantExecutionError) as error:
                await execute_quant(request.model_copy(update=change), 10)
            assert error.value.code == code
            assert not request.output_path.exists()

    asyncio.run(scenario())


def test_protocol_limits_and_malformed_worker_are_rejected(
    snapshot: Snapshot, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stock_research.quant_execution import bounded_report
    from stock_research.schemas.quant_execution import MAX_QUANT_REPORT, MAX_QUANT_REQUEST, MAX_QUANT_RESPONSE

    async def scenario() -> None:
        _settings, path = sample_settings(tmp_path, snapshot)
        request = QuantRequest(
            strategy="martin",
            snapshot_path=path.resolve(),
            snapshot_id=snapshot.id,
            snapshot_hash=snapshot.content_hash,
            source_hash=LOADED_SOURCE_IDENTITY,
            output_path=(tmp_path / "result.json").resolve(),
            parameters=MartinParameters(),
        )
        original, children = asyncio.create_subprocess_exec, []
        for payload, pause in ((b"x" * (MAX_QUANT_RESPONSE + 1), 5), (b"invalid\ninvalid\n", 0)):
            code = f"import sys,time; sys.stdin.buffer.readline(); sys.stdout.buffer.write({payload!r}); sys.stdout.buffer.flush(); time.sleep({pause})"

            async def malformed(*_args, code: str = code, **kwargs):
                child = await original(sys.executable, "-c", code, **kwargs)
                children.append(child)
                return child

            monkeypatch.setattr(asyncio, "create_subprocess_exec", malformed)
            with pytest.raises(QuantExecutionError) as error:
                await execute_quant(request, 10)
            assert error.value.code == "quant_output_invalid"
            assert children[-1].returncode is not None
        with pytest.raises(QuantExecutionError) as error:
            await execute_quant(request.model_copy(update={"snapshot_path": Path("/" + "a" * MAX_QUANT_REQUEST)}), 10)
        assert error.value.code == "quant_input_invalid" and len(children) == 2
        oversized = tmp_path / "oversized.json"
        with oversized.open("wb") as output:
            output.truncate(MAX_QUANT_REPORT + 1)
        with pytest.raises(QuantExecutionError) as error:
            bounded_report(oversized)
        assert error.value.code == "quant_output_invalid"
        assert not request.output_path.exists()

    asyncio.run(scenario())
