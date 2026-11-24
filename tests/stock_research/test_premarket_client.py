"""固定API客户端的协议边界、未知结果回查与有界workflow收尾。"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from pydantic import ValidationError

from stock_research.identity import new_id
from stock_research.models import Snapshot
from stock_research.premarket import plan_premarket
from stock_research.premarket_workflow import read_confirmed, recoverable, run_workflow
from stock_research.schemas.api import ApiResponse, Code, ReportView, RunView, StageView
from stock_research.schemas.premarket import PremarketBinding, PremarketInput, PremarketPlan, PremarketReceipt
from stock_research.workflow_client import PremarketClient, WorkflowError
from stock_research.workflow_settings import WorkflowSettings

FIXED = datetime(2026, 11, 23, 13, tzinfo=UTC)
CREDENTIAL = "synthetic-workflow-identity-over-32-characters"


def data() -> PremarketInput:
    return PremarketInput.model_validate(
        {
            "schedule": {
                "schedule_id": "stock-research-premarket",
                "event_id": "stock-research-premarket:2026-11-23T13:00:00Z",
                "scheduled_for": "2026-11-23T13:00:00Z",
                "workspace_id": "ws-beta",
                "trigger_kind": "scheduled",
            }
        }
    )


def pending() -> PremarketReceipt:
    return PremarketReceipt(
        job_id=new_id(),
        plan=PremarketPlan.model_validate(plan_premarket(data(), FIXED)),
        origin=None,
        status="unconfirmed",
        preparation=None,
        run=None,
        report=None,
    )


def packet(receipt: PremarketReceipt) -> bytes:
    return (
        ApiResponse[PremarketReceipt](code=Code.OK, message="ok", data=receipt, correlation_id=new_id())
        .model_dump_json()
        .encode()
    )


def test_dedicated_configuration_never_falls_back_to_query_or_server_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".stock-research").mkdir()
    (tmp_path / ".env").write_text("STOCK_RESEARCH_POSTGRES_PORT=invalid-must-not-load\n")
    (tmp_path / ".stock-research/query.env").write_text("STOCK_RESEARCH_QUERY_TOKEN=" + CREDENTIAL)
    with pytest.raises(ValidationError):
        WorkflowSettings()
    monkeypatch.setenv("STOCK_RESEARCH_WORKFLOW_TOKEN", CREDENTIAL)
    settings = WorkflowSettings()
    assert settings.endpoint == "http://127.0.0.1:8788" and set(WorkflowSettings.model_fields) == {
        "host",
        "port",
        "token",
    }
    assert settings.token.get_secret_value() == CREDENTIAL


@pytest.mark.parametrize(
    "failure,code",
    [
        ("redirect", "request_denied"),
        ("denied", "request_denied"),
        ("busy", "request_conflict"),
        ("unavailable", "request_unavailable"),
        ("transport", "transport_unknown"),
        ("invalid", "response_invalid"),
        ("large", "response_too_large"),
        ("scope", "scope_mismatch"),
    ],
)
def test_client_rejects_untrusted_response_without_reflecting_secrets(failure: str, code: str) -> None:
    async def scenario() -> None:
        calls = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            if failure == "transport":
                raise httpx.ConnectError("private-fixture-" + CREDENTIAL)
            if failure == "scope":
                receipt = pending()
                changed = receipt.plan.schedule.model_copy(update={"workspace_id": "other"})
                return httpx.Response(
                    200,
                    content=packet(
                        receipt.model_copy(update={"plan": receipt.plan.model_copy(update={"schedule": changed})})
                    ),
                )
            status = {"redirect": 302, "denied": 403, "busy": 409, "unavailable": 503}.get(failure, 200)
            content = b"x" * 2_000_001 if failure == "large" else ("private-fixture-" + CREDENTIAL).encode()
            return httpx.Response(status, content=content, headers={"Location": "https://untrusted.invalid/collect"})

        client = PremarketClient(
            WorkflowSettings(_env_file=None, token=CREDENTIAL), data(), transport=httpx.MockTransport(respond)
        )
        with pytest.raises(WorkflowError) as raised:
            await client.call("POST")
        assert (
            raised.value.code == code
            and CREDENTIAL not in str(raised.value)
            and "private-fixture" not in str(raised.value)
        )
        assert len(calls) == 1
        assert calls[0].url.host == "127.0.0.1" and calls[0].headers["Authorization"] == "Bearer " + CREDENTIAL
        assert calls[0].read() == httpx.Request("POST", "http://unused", json=data().model_dump(mode="json")).read()
        with pytest.raises(WorkflowError, match="workflow_action_invalid"):
            await client.call("GET", job_id="../../private")
        assert len(calls) == 1

    asyncio.run(scenario())


class ScriptedClient:
    def __init__(self, script: list[PremarketReceipt | WorkflowError]) -> None:
        self.data = data()
        self.script = list(script)
        self.calls = []

    async def call(self, method: str, *, job_id: UUID | None = None, action: str | None = None) -> PremarketReceipt:
        self.calls.append((method, job_id, action))
        result = self.script.pop(0)
        if isinstance(result, WorkflowError):
            raise result
        return result


def test_observation_errors_only_repeat_get_on_the_same_job(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        original_sleep = asyncio.sleep

        async def fast_sleep(_delay: float) -> None:
            await original_sleep(0)

        monkeypatch.setattr("stock_research.premarket_workflow.asyncio.sleep", fast_sleep)
        receipt = pending()
        client = ScriptedClient([WorkflowError("transport_unknown"), WorkflowError("request_unavailable"), receipt])
        result = await read_confirmed(client, receipt.job_id)
        assert result == receipt and client.calls == [("GET", receipt.job_id, None)] * 3

    asyncio.run(scenario())


def test_unconfirmed_preparation_replays_the_original_intent_with_two_backoffs(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        receipt = pending()
        client = ScriptedClient([receipt, receipt, receipt])
        delays = []
        original_sleep = asyncio.sleep

        async def fast_sleep(delay: float) -> None:
            delays.append(delay)
            await original_sleep(0)

        monkeypatch.setattr("stock_research.premarket_workflow.asyncio.sleep", fast_sleep)
        monkeypatch.setattr("stock_research.premarket_workflow.now", lambda: FIXED)
        outcome = await run_workflow(data(), WorkflowSettings(_env_file=None, token=CREDENTIAL), client=client)
        assert delays == [60, 180] and client.calls == [("POST", None, None)] * 3
        assert outcome.receipt.status == "unconfirmed" and outcome.delivery == "not_requested"

    asyncio.run(scenario())


def test_owner_cancellation_persists_only_known_original_job(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        receipt = pending()
        cancelled = receipt.model_copy(update={"status": "cancelled"})
        client = ScriptedClient([receipt, cancelled])
        monkeypatch.setattr("stock_research.premarket_workflow.now", lambda: FIXED)
        task = asyncio.create_task(
            run_workflow(data(), WorkflowSettings(_env_file=None, token=CREDENTIAL), client=client)
        )
        async with asyncio.timeout(1):
            while not client.calls:
                await asyncio.sleep(0)
        task.cancel()
        outcome = await asyncio.wait_for(task, 1)
        assert outcome.error_code == "workflow_cancelled" and outcome.receipt.status == "cancelled"
        assert client.calls == [("POST", None, None), ("POST", receipt.job_id, "cancel")]

    asyncio.run(scenario())


def running_receipt(snapshot: Snapshot) -> PremarketReceipt:
    receipt = pending()
    run = RunView(
        run_id=new_id(),
        market="us",
        target_session=receipt.plan.target_session,
        as_of=snapshot.as_of,
        quality=snapshot.quality,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        report_due=FIXED + timedelta(minutes=30),
        observed_at=FIXED,
        late=False,
        preview=True,
        status="running",
        error_code=None,
        companies=(),
        selections=(),
        reviews=(),
        stages=(),
    )
    report = ReportView(report_id=new_id(), revision=1, create_time=FIXED, content_hash="a" * 64, run=run)
    origin = PremarketBinding(
        job_id=receipt.job_id, schedule=data().schedule, execution_deadline=data().schedule.execution_deadline
    )
    return receipt.model_copy(update={"status": "running", "origin": origin, "run": run, "report": report})


def test_original_deadline_stops_polling_and_confirms_remote_cleanup(
    snapshot: Snapshot, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        receipt = running_receipt(snapshot)
        stopped = receipt.model_copy(
            update={"status": "cancelled", "run": receipt.run.model_copy(update={"status": "cancelled"})}
        )
        client = ScriptedClient([receipt, stopped])
        monkeypatch.setattr(
            "stock_research.premarket_workflow.now", lambda: FIXED + timedelta(minutes=50, milliseconds=-20)
        )
        outcome = await run_workflow(data(), WorkflowSettings(_env_file=None, token=CREDENTIAL), client=client)
        assert outcome.error_code == "workflow_deadline" and outcome.receipt.status == "cancelled"
        assert client.calls == [("POST", None, None), ("POST", receipt.job_id, "cancel")]

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "error,allowed",
    [
        ("transient_provider_error", True),
        ("transport_error", True),
        ("invalid_selection", False),
        ("executor_configuration_failed", False),
        (None, False),
    ],
)
def test_external_recovery_does_not_retry_structural_or_configuration_failures(
    snapshot: Snapshot, error: str | None, allowed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("stock_research.premarket_workflow.now", lambda: FIXED)
    receipt = running_receipt(snapshot)
    stage = StageView(
        id=new_id(),
        strategy="martin",
        kind="select",
        status="failed",
        attempt_count=1,
        target_selection_id=None,
        context_hash=None,
        error=error,
        receipt_available=False,
    )
    changed = receipt.model_copy(
        update={"status": "failed", "run": receipt.run.model_copy(update={"status": "failed", "stages": (stage,)})}
    )
    assert recoverable(changed) is allowed
    monkeypatch.setattr("stock_research.premarket_workflow.now", lambda: FIXED + timedelta(hours=1))
    assert recoverable(changed) is False


@pytest.mark.parametrize("kind", ["missing", "symlink", "wide", "directory"])
def test_operator_identity_path_is_explicit_and_private(tmp_path: Path, kind: str) -> None:
    from stock_research.workflow_settings import load_workflow_identity

    identity = tmp_path / "workflow.env"
    if kind == "directory":
        identity.mkdir()
    elif kind == "symlink":
        identity.symlink_to(tmp_path / "missing-target")
    elif kind == "wide":
        identity.write_text("STOCK_RESEARCH_WORKFLOW_TOKEN=" + CREDENTIAL)
        identity.chmod(0o644)
    with pytest.raises(ValueError, match="0600"):
        load_workflow_identity(identity)


def test_plan_cannot_load_an_identity_or_override_execution_clock(tmp_path: Path) -> None:
    from stock_research.premarket import main

    for arguments in (
        ["--plan", "--identity-file", str(tmp_path / "private.env")],
        ["--execute", "--at", FIXED.isoformat()],
    ):
        with pytest.raises(SystemExit) as raised:
            main(arguments)
        assert raised.value.code == 2


@pytest.mark.parametrize(
    "stamp", ["1980-11-23T13:00:00Z", "1990-01-01T13:00:00Z", "1990-01-02T13:00:00Z", "2041-11-23T13:00:00Z"]
)
def test_out_of_calendar_dates_are_rejected_before_admission(stamp: str) -> None:
    payload = data().model_dump(mode="json")
    payload["schedule"].update(
        event_id="stock-research-premarket:manual-" + stamp, scheduled_for=stamp, trigger_kind="manual"
    )
    with pytest.raises(ValidationError, match="日历范围"):
        PremarketInput.model_validate(payload)


def test_equivalent_timezone_input_has_one_identity_and_lower_run_budget_wins() -> None:
    from stock_research.errors import ResearchIntegrityError
    from stock_research.execution_budget import run_deadline
    from stock_research.premarket_service import request_identity
    from stock_research.schemas.premarket import PremarketBinding

    original = data()
    payload = original.model_dump(mode="json")
    payload["schedule"]["scheduled_for"] = "2026-11-23T08:00:00-05:00"
    assert request_identity(PremarketInput.model_validate(payload)) == request_identity(original)
    binding = PremarketBinding(
        job_id=new_id(), schedule=original.schedule, execution_deadline=original.schedule.execution_deadline
    )
    row = {"create_time": FIXED, "payload": {"premarket": binding.model_dump(mode="json")}}
    assert run_deadline(row, 1) == FIXED + timedelta(seconds=1)
    assert run_deadline(row, 3000) == original.schedule.execution_deadline
    row["payload"]["premarket"]["execution_deadline"] = (FIXED + timedelta(days=1)).isoformat()
    with pytest.raises(ResearchIntegrityError):
        run_deadline(row, 3000)


@pytest.mark.parametrize("finished", [True, False])
def test_cancel_conflict_is_reconciled_without_claiming_a_running_job_stopped(
    snapshot: Snapshot, finished: bool
) -> None:
    from stock_research.premarket_workflow import stop_job

    async def scenario() -> None:
        receipt = running_receipt(snapshot)
        confirmed = (
            receipt.model_copy(
                update={"status": "completed", "run": receipt.run.model_copy(update={"status": "completed"})}
            )
            if finished
            else receipt
        )
        client = ScriptedClient([WorkflowError("request_conflict"), confirmed])
        if finished:
            assert (await stop_job(client, receipt)).status == "completed"
        else:
            with pytest.raises(WorkflowError, match="cancel_unconfirmed"):
                await stop_job(client, receipt)
        assert client.calls == [("POST", receipt.job_id, "cancel"), ("GET", receipt.job_id, None)]

    asyncio.run(scenario())


def test_unknown_admission_keeps_original_intent_and_never_guesses_a_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        original_sleep = asyncio.sleep

        async def fast_sleep(_delay: float) -> None:
            await original_sleep(0)

        monkeypatch.setattr("stock_research.premarket_workflow.asyncio.sleep", fast_sleep)
        monkeypatch.setattr("stock_research.premarket_workflow.now", lambda: FIXED)
        client = ScriptedClient([WorkflowError("transport_unknown") for _ in range(3)])
        outcome = await run_workflow(data(), WorkflowSettings(_env_file=None, token=CREDENTIAL), client=client)
        assert outcome.error_code == "admission_unconfirmed" and outcome.receipt is None
        assert client.calls == [("POST", None, None)] * 3

    asyncio.run(scenario())
