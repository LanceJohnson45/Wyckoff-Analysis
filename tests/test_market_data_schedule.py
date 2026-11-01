"""定时拉取按行情目标日判断，允许跨午夜延迟和手动补数。"""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from scripts.market_data_schedule import schedule_decision


@pytest.mark.parametrize(
    "now,event,calendar,expected,reason,target",
    [
        pytest.param("2026-11-01T18:00:00", "schedule", [], False, "target_is_weekend", "2026-11-01", id="sunday"),
        pytest.param("2026-10-31T00:36:00", "schedule", [date(2026, 10, 30)], True, "target_is_trading_day", "2026-10-30", id="delayed-friday"),
        pytest.param("2026-10-01T18:00:00", "schedule", [date(2026, 9, 30), date(2026, 10, 9)], False, "target_is_holiday", "2026-10-01", id="weekday-holiday"),
        pytest.param("2026-11-01T18:00:00", "workflow_dispatch", [], True, "manual_backfill", "2026-11-01", id="manual-backfill"),
    ],
)
def test_normal_schedule_decision(now: str, event: str, calendar: list[date], expected: bool, reason: str, target: str) -> None:
    """【正常值】周末节假日跳过，延迟任务和手动补数保留。"""
    result = schedule_decision(event=event, market="cn", now=datetime.fromisoformat(now).replace(tzinfo=ZoneInfo("Asia/Shanghai")), load_calendar=lambda: calendar)

    assert result["should_run"] is expected
    assert result["reason"] == reason
    assert result["target_date"] == target


def test_error_stale_calendar() -> None:
    """【异常值】日历过期时明确失败，避免误判休市。"""
    with pytest.raises(RuntimeError, match="未覆盖目标日"):
        schedule_decision(event="schedule", market="cn", now=datetime(2026, 10, 30, 18, tzinfo=ZoneInfo("Asia/Shanghai")), load_calendar=lambda: [date(2025, 12, 31)])


def test_normal_cli_writes_workflow_output(monkeypatch: pytest.MonkeyPatch, tmp_path, capsys: pytest.CaptureFixture[str]) -> None:
    """【正常值】真实入口输出结构化调度日志和可消费的 Actions output。"""
    import json
    from scripts import market_data_schedule

    output = tmp_path / "github-output"
    monkeypatch.setenv("GITHUB_EVENT_NAME", "workflow_dispatch")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setattr("sys.argv", ["market_data_schedule", "--market", "cn"])

    status = market_data_schedule.main()
    record = json.loads(capsys.readouterr().out)

    assert status == 0
    assert record["event"] == "market_data_schedule"
    assert record["trigger_event"] == "workflow_dispatch"
    assert "should_run=true\n" in output.read_text()
    assert f"target_date={record['target_date']}\n" in output.read_text()
