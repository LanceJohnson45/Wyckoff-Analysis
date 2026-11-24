"""单用户MVP的定点发布、持久冻结与真实子进程边界。"""

import asyncio
import json
import os
import shlex
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from stock_research import mvp, mvp_delivery
from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.mvp_delivery import Publication, call_worker, freeze_publication, publication
from stock_research.mvp_settings import MvpSettings, load_mvp_settings
from stock_research.premarket import plan_premarket
from stock_research.premarket_workflow import WorkflowOutcome
from stock_research.schemas.api import ReportView, RunView, StageView
from stock_research.schemas.premarket import PremarketBinding, PremarketInput, PremarketPlan, PremarketReceipt

FIXED = datetime(2026, 11, 23, 13, 30, tzinfo=UTC)


@pytest.fixture
def settings(tmp_path: Path) -> MvpSettings:
    return MvpSettings(_env_file=None, **{key: tmp_path / key for key in MvpSettings.model_fields})


@pytest.fixture
def data() -> PremarketInput:
    return PremarketInput.model_validate(
        {
            "schedule": {
                "schedule_id": "stock-research-premarket",
                "event_id": "stock-research-premarket:2026-11-23T13:00:00Z",
                "scheduled_for": "2026-11-23T13:00:00Z",
                "workspace_id": "self",
                "trigger_kind": "scheduled",
            }
        }
    )


def completed(data: PremarketInput, snapshot: Snapshot) -> PremarketReceipt:
    job_id = new_id()
    run = RunView(
        run_id=new_id(),
        market="us",
        target_session=data.schedule.target_session,
        as_of=snapshot.as_of,
        quality=snapshot.quality,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        report_due=FIXED,
        observed_at=FIXED,
        late=False,
        preview=False,
        status="completed",
        error_code=None,
        companies=(),
        selections=(),
        reviews=(),
        stages=(
            StageView(
                id=new_id(),
                strategy="martin",
                kind="select",
                status="completed",
                attempt_count=1,
                target_selection_id=None,
                context_hash=None,
                error=None,
                receipt_available=True,
            ),
        ),
    )
    return PremarketReceipt(
        job_id=job_id,
        plan=PremarketPlan.model_validate(plan_premarket(data, FIXED)),
        origin=PremarketBinding(
            job_id=job_id, schedule=data.schedule, execution_deadline=data.schedule.execution_deadline
        ),
        status="completed",
        preparation=None,
        run=run,
        report=ReportView(report_id=new_id(), revision=1, create_time=FIXED, content_hash="a" * 64, run=run),
    )


def test_due_publishes_even_while_pipeline_waits_then_one_recovery(
    settings: MvpSettings,
    data: PremarketInput,
    snapshot: Snapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        initial_sent = asyncio.Event()
        transmitted = []
        receipt = completed(data, snapshot)

        async def pipeline(_settings, _data, _plan, progress):
            await initial_sent.wait()
            progress.append(receipt)
            return {"status": "completed"}, WorkflowOutcome(receipt=receipt, error_code=None)

        async def transport(_settings, kind, payload, _timeout):
            assert kind == "notify"
            transmitted.append(payload)
            initial_sent.set()
            return {"state": "sent", "message_ref": "om_synthetic"}

        monkeypatch.setattr(mvp, "now", lambda: FIXED)
        monkeypatch.setattr(mvp, "research_pipeline", pipeline)
        monkeypatch.setattr(mvp_delivery, "call_worker", transport)
        result = await asyncio.wait_for(mvp.run_mvp(settings, data), 2)
        assert len(transmitted) == 2
        assert "尚无可发布" in transmitted[0]["body"]
        assert str(receipt.report.report_id) in transmitted[1]["body"]
        assert transmitted[0]["window"].endswith(":initial") and transmitted[1]["window"].endswith(":recovery")
        assert result["initial"]["state"] == result["recovery"]["state"] == "sent"

    asyncio.run(scenario())


def test_report_ready_before_due_does_not_publish_early(
    settings: MvpSettings,
    data: PremarketInput,
    snapshot: Snapshot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        due = asyncio.Event()
        transmitted = []
        original_sleep = asyncio.sleep
        receipt = completed(data, snapshot)

        async def pipeline(*_args):
            return {"status": "completed"}, WorkflowOutcome(receipt=receipt, error_code=None)

        async def clock_sleep(seconds):
            assert seconds == 1800
            await due.wait()

        async def transport(_settings, _kind, payload, _timeout):
            transmitted.append(payload)
            return {"state": "sent", "message_ref": "om_synthetic"}

        monkeypatch.setattr(mvp, "now", lambda: datetime(2026, 11, 23, 13, tzinfo=UTC))
        monkeypatch.setattr(mvp, "research_pipeline", pipeline)
        monkeypatch.setattr(mvp.asyncio, "sleep", clock_sleep)
        monkeypatch.setattr(mvp_delivery, "call_worker", transport)
        task = asyncio.create_task(mvp.run_mvp(settings, data))
        await original_sleep(0)
        await original_sleep(0)
        assert not transmitted
        due.set()
        result = await asyncio.wait_for(task, 2)
        assert len(transmitted) == 1 and result["recovery"] is None

    asyncio.run(scenario())


def test_same_day_publication_remains_frozen_across_replay(tmp_path: Path) -> None:
    first = Publication(body="原始部分报告", completed_stages=1, report_id=new_id())
    second = Publication(body="重算结果不能覆盖首报", completed_stages=4, report_id=new_id())
    assert freeze_publication(tmp_path, first) == first
    assert freeze_publication(tmp_path, second) == first
    assert (tmp_path / "publication.json").stat().st_mode & 0o077 == 0


def test_no_report_does_not_invent_empty_selections(data: PremarketInput) -> None:
    value = publication(data, None, "initial", "source_refresh_unconfirmed")
    assert value.report_id is None and value.completed_stages == 0
    assert "source_refresh_unconfirmed" in value.body and "尚无可发布" in value.body


def test_private_configuration_rejects_public_file(settings: MvpSettings, tmp_path: Path) -> None:
    path = tmp_path / "mvp.env"
    path.write_text(
        "\n".join(
            "STOCK_RESEARCH_MVP_" + key.upper() + "=" + str(value) for key, value in settings.model_dump().items()
        )
    )
    path.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        load_mvp_settings(path)
    path.chmod(0o600)
    assert load_mvp_settings(path) == settings


@pytest.mark.parametrize("stall", [False, True])
def test_real_worker_isolates_credentials_and_reaps_timed_out_child(
    settings: MvpSettings,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stall: bool,
) -> None:
    root = settings.source_root
    (root / ".venv/bin").mkdir(parents=True)
    executable = root / ".venv/bin/python"
    executable.write_text("#!/bin/sh\nexec " + shlex.quote(sys.executable) + ' "$@"\n')
    executable.chmod(0o755)
    (root / "scripts").mkdir()
    (root / "scripts/__init__.py").touch()
    marker = tmp_path / "child.json"
    (root / "scripts/us_research_refresh.py").write_text(
        "import os,json,time\nfrom pathlib import Path\n"
        "def refresh_research(symbols, as_of):\n"
        f" Path({str(marker)!r}).write_text(json.dumps({{'pid':os.getpid(),'keys':sorted(os.environ)}}))\n"
        + (" time.sleep(60)\n" if stall else "")
        + " return {'status':'completed','as_of':str(as_of),'symbols':symbols}\n"
    )
    settings.source_env_file.write_text("PG_HOST=fixture\nSA_INTERNAL_TOKEN=not-loaded\nMODEL_API_KEY=not-loaded\n")
    settings.source_env_file.chmod(0o600)
    monkeypatch.setenv("SA_PG_URL", "parent-must-not-inherit")
    monkeypatch.setenv("MODEL_API_KEY", "parent-must-not-inherit")

    async def scenario():
        operation = call_worker(settings, "refresh", {"symbols": ["AAPL"], "as_of": "2026-11-20"}, 3 if stall else 10)
        if stall:
            with pytest.raises(TimeoutError):
                await operation
        else:
            result = await operation
            assert result["status"] == "completed" and result["symbols"] == ["AAPL"], result

    asyncio.run(scenario())
    observed = json.loads(marker.read_bytes())
    assert "PG_HOST" in observed["keys"]
    assert not {"SA_PG_URL", "SA_INTERNAL_TOKEN", "MODEL_API_KEY"} & set(observed["keys"])
    with pytest.raises(ProcessLookupError):
        os.kill(observed["pid"], 0)
