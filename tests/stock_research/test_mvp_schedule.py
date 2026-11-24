"""Real-time metadata is derived from New York's calendar, including DST."""

from datetime import UTC, datetime

import pytest

from scripts.stock_research_mvp_schedule import daily_input, finished
from stock_research.premarket import plan_premarket


@pytest.mark.parametrize(
    "stamp,slot",
    [
        ("2026-11-24T13:02:00+00:00", "2026-11-24T13:00:00+00:00"),
        ("2026-07-06T12:02:00+00:00", "2026-07-06T12:00:00+00:00"),
    ],
)
def test_tick_preserves_nominal_slot_and_never_extends_deadline(stamp: str, slot: str) -> None:
    observed = datetime.fromisoformat(stamp)
    value = daily_input(observed, "self")
    assert value.schedule.scheduled_for == datetime.fromisoformat(slot)
    plan = plan_premarket(value, observed)
    assert plan["budget_seconds"] == 48 * 60
    assert plan["status"] == "within_window"


def test_expired_and_holiday_ticks_do_not_roll_to_a_new_session() -> None:
    expired = datetime(2026, 11, 24, 14, tzinfo=UTC)
    assert plan_premarket(daily_input(expired, "self"), expired)["status"] == "expired"
    holiday = datetime(2026, 11, 26, 13, tzinfo=UTC)
    assert plan_premarket(daily_input(holiday, "self"), holiday)["status"] == "skipped_non_session"


def test_manual_retains_actual_event_time_and_delivery_unknown_is_not_finished() -> None:
    observed = datetime(2026, 11, 24, 3, 40, 23, tzinfo=UTC)
    data = daily_input(observed, "self", manual=True)
    assert data.schedule.scheduled_for == observed and "manual-" in data.schedule.event_id
    result = {"initial": {"state": "unknown"}, "research": {"receipt": {"status": "completed"}}}
    assert not finished(result)
    result["initial"]["state"] = "sent"
    assert finished(result)
