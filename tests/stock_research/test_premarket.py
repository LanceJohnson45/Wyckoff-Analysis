"""正式交易日历、原计划归属和真正独立固定argv计划入口。"""

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from stock_research.premarket import PremarketInput, plan_premarket, workflow_paths


def context(stamp: str = "2026-11-23T13:00:00Z", kind: str = "scheduled") -> dict[str, dict[str, str]]:
    return {
        "schedule": {
            "schedule_id": "stock-research-premarket",
            "event_id": "stock-research-premarket:" + ("manual-" if kind == "manual" else "") + stamp,
            "scheduled_for": stamp,
            "workspace_id": "ws-beta",
            "trigger_kind": kind,
        }
    }


@pytest.mark.parametrize(
    ("slot", "previous", "due"),
    [
        ("2026-05-04T12:00:00Z", "2026-05-01", "2026-05-04T20:30:00+08:00"),
        ("2026-11-23T13:00:00Z", "2026-11-20", "2026-11-23T21:30:00+08:00"),
        ("2026-11-27T13:00:00Z", "2026-11-25", "2026-11-27T21:30:00+08:00"),
    ],
)
def test_calendar_dst_and_half_day_keep_previous_complete_session(slot: str, previous: str, due: str) -> None:
    result = plan_premarket(PremarketInput.model_validate(context(slot)), datetime.fromisoformat(slot))
    assert result["status"] == "within_window" and result["budget_seconds"] == 3000
    assert result["as_of"] == previous and result["report_due"] == due
    assert not result["research_started"] and not result["late"]


def test_holiday_is_skipped_without_rolling_to_another_date() -> None:
    stamp = "2026-11-26T13:00:00Z"
    result = plan_premarket(PremarketInput.model_validate(context(stamp)), datetime.fromisoformat(stamp))
    assert result["target_session"] == "2026-11-26" and result["status"] == "skipped_non_session"
    assert result["as_of"] is None and result["execution_deadline"] is None and result["budget_seconds"] == 0


@pytest.mark.parametrize(
    ("at", "status", "budget", "late"),
    [
        ("2026-11-23T12:59:59Z", "before_start", 0, False),
        ("2026-11-23T13:30:00Z", "within_window", 1200, False),
        ("2026-11-23T13:49:59Z", "within_window", 1, True),
        ("2026-11-23T13:50:00Z", "expired", 0, True),
        ("2026-11-24T13:00:00Z", "expired", 0, True),
    ],
)
def test_delayed_dispatch_never_moves_original_day_or_extends_0850(
    at: str, status: str, budget: int, late: bool
) -> None:
    result = plan_premarket(PremarketInput.model_validate(context()), datetime.fromisoformat(at))
    assert result["target_session"] == "2026-11-23" and result["as_of"] == "2026-11-20"
    assert (result["status"], result["budget_seconds"], result["late"]) == (status, budget, late)
    if budget == 0:
        assert result["execution_deadline"] is None


def test_explicit_late_manual_is_labelled_and_has_its_own_bounded_plan() -> None:
    stamp = "2026-11-23T15:00:00Z"
    result = plan_premarket(PremarketInput.model_validate(context(stamp, "manual")), datetime.fromisoformat(stamp))
    assert result["status"] == "late_manual" and result["late"] and result["budget_seconds"] == 3000
    assert result["research_started"] is False
    with pytest.raises(ValueError, match="时区"):
        plan_premarket(PremarketInput.model_validate(context()), datetime(2026, 11, 23, 13))


@pytest.mark.parametrize(
    ("observed", "status", "budget"),
    [
        ("2026-11-23T15:49:59Z", "late_manual", 1),
        ("2026-11-23T15:50:00Z", "expired", 0),
        ("2026-11-24T15:00:00Z", "expired", 0),
    ],
)
def test_manual_replay_never_resets_original_3000_second_budget(observed: str, status: str, budget: int) -> None:
    data = PremarketInput.model_validate(context("2026-11-23T15:00:00Z", "manual"))
    result = plan_premarket(data, datetime.fromisoformat(observed))
    assert result["target_session"] == "2026-11-23" and result["late"]
    assert result["status"] == status and result["budget_seconds"] == budget


@pytest.mark.parametrize("change", ["date", "extra", "workflow", "naive", "wrong-hour", "manual-id"])
def test_untrusted_or_incoherent_schedule_context_is_rejected(change: str) -> None:
    data = context()
    if change == "date":
        data["schedule"]["scheduled_for"] = "2026-11-24T13:00:00Z"
    elif change == "extra":
        data["argv"] = ["untrusted-command"]
    elif change == "workflow":
        data["schedule"]["schedule_id"] = "other-task"
    elif change == "naive":
        data["schedule"]["scheduled_for"] = "2026-11-23T13:00:00"
    elif change == "wrong-hour":
        data = context("2026-11-23T14:00:00Z")
    else:
        data["schedule"]["trigger_kind"] = "manual"
    with pytest.raises(ValueError):
        PremarketInput.model_validate(data)


def test_fixed_argv_entry_reads_runner_input_writes_private_plan_and_ignores_env(tmp_path: Path) -> None:
    root = tmp_path / "runs"
    run = root / "synthetic-run"
    run.mkdir(parents=True)
    source, output = run / "input.json", run / "output.json"
    source.write_text(json.dumps(context()))
    (tmp_path / ".env").write_text("STOCK_RESEARCH_POSTGRES_PORT=invalid-must-not-load\n")
    environment = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
        "SA_WORKFLOW_NAME": "stock-research-premarket",
        "SA_RUN_ARTIFACTS": str(root),
        "SA_RUN_INPUT_JSON": str(source),
        "SA_RUN_OUTPUT_JSON": str(output),
        "STOCK_RESEARCH_POSTGRES_PORT": "invalid-must-not-load",
    }
    command = [sys.executable, "-m", "stock_research.premarket", "--plan", "--at", "2026-11-24T13:00:00Z"]
    result = subprocess.run(command, env=environment, cwd=tmp_path, text=True, capture_output=True, timeout=20)
    assert result.returncode == 0, result.stderr
    plan = json.loads(output.read_text())
    assert json.loads(result.stdout) == plan
    assert plan["status"] == "expired" and plan["target_session"] == "2026-11-23"
    assert plan["research_started"] is False and output.stat().st_mode & 0o777 == 0o600
    output.unlink()
    source.write_text('{"private": "must-not-echo-credential",')
    failed = subprocess.run(command, env=environment, cwd=tmp_path, text=True, capture_output=True, timeout=20)
    assert failed.returncode == 1 and not output.exists()
    assert "must-not-echo-credential" not in failed.stdout + failed.stderr
    outside = tmp_path / "outside.json"
    outside.write_text("{}")
    source.unlink()
    source.symlink_to(outside)
    with pytest.raises(ValueError, match="身份"):
        workflow_paths(environment)
    source.unlink()
    source.write_text("{}")
    environment["SA_RUN_OUTPUT_JSON"] = str(tmp_path / "output.json")
    with pytest.raises(ValueError, match="越界"):
        workflow_paths(environment)
    assert not (tmp_path / "output.json").exists()
    assert os.environ.get("SA_WORKFLOW_NAME") != "stock-research-premarket"
