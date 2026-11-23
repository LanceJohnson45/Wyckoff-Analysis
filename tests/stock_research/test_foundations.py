"""研究基础模块的版本、行情约束和日历边界验证。"""

from datetime import date, datetime

import pytest
from pydantic import ValidationError

from stock_research.calendar import latest_complete_session, session_times, sessions_ending
from stock_research.config import MarketSourceSettings, Settings
from stock_research.identity import SHANGHAI, digest
from stock_research.models import Bar, UniverseMember, UniverseVersion, normalize_symbol


def member(**overrides: object) -> UniverseMember:
    """构造有明确来源和生效时间的测试成员。"""
    return UniverseMember.model_validate(
        {
            "symbol": "BRK.B",
            "name": "Berkshire Hathaway",
            "index": "sp500",
            "source": "fixture",
            "effective_date": "2026-01-01",
            "obtained_time": "2026-01-01T08:00:00+08:00",
            **overrides,
        }
    )


@pytest.mark.parametrize("raw", ["BRK.B", "US:BRK-B", "BRK-B.US", " brk.b "])
def test_vendor_symbols_share_one_identity(raw: str) -> None:
    assert normalize_symbol(raw) == "BRK-B"


def test_universe_retains_membership_and_excludes_expired_members() -> None:
    universe = UniverseVersion(
        version="fixture-v1",
        members=(member(), member(index="nasdaq100"), member(symbol="OLD", expiry_date="2026-11-20")),
    )
    active = universe.active(date(2026, 11, 20))
    assert list(active) == ["BRK-B"]
    assert {item.index for item in active["BRK-B"]} == {"sp500", "nasdaq100"}


def test_duplicate_universe_member_is_rejected() -> None:
    with pytest.raises(ValidationError, match="重复成员"):
        UniverseVersion(version="fixture-v1", members=(member(), member()))


@pytest.mark.parametrize(
    "overrides", [{"low": 12}, {"volume": -1}, {"close": float("nan")}, {"source_revision": "2026-11-20T12:00:00"}]
)
def test_invalid_daily_bar_is_rejected(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Bar.model_validate(
            {
                "symbol": "AAPL",
                "kind": "stock",
                "session_date": "2026-11-20",
                "open": 10,
                "high": 12,
                "low": 9,
                "close": 11,
                "volume": 1000,
                "source": "fixture",
                "source_revision": "2026-11-21T08:00:00+08:00",
                "observed_time": "2026-11-21T09:00:00+08:00",
                **overrides,
            }
        )


@pytest.mark.parametrize("day,hour", [(date(2026, 7, 1), 21), (date(2026, 11, 23), 22)])
def test_report_deadline_follows_new_york_dst(day: date, hour: int) -> None:
    opening, deadline, previous = session_times(day)
    assert (opening.hour, opening.minute) == (hour, 30)
    assert (opening - deadline).total_seconds() == 3600
    assert previous < day
    assert opening.tzinfo == SHANGHAI


def test_thanksgiving_is_not_a_research_session() -> None:
    with pytest.raises(ValueError, match="不是 XNYS 交易日"):
        session_times(date(2026, 11, 26))
    assert sessions_ending(date(2026, 11, 27), 2) == [date(2026, 11, 25), date(2026, 11, 27)]


def test_test_database_is_separate_and_time_is_aware() -> None:
    settings = Settings(_env_file=None)
    assert settings.database_url(test=True).database == "stock_research_test"
    assert settings.database_url().drivername == "postgresql+asyncpg"
    assert datetime.now(SHANGHAI).utcoffset().total_seconds() == 28800


def test_content_hash_is_order_independent_and_rejects_nan() -> None:
    assert digest({"b": 2, "a": 1}) == digest({"a": 1, "b": 2})
    with pytest.raises(ValueError):
        digest({"close": float("nan")})


@pytest.mark.parametrize(
    "at,expected",
    [
        pytest.param("2026-11-23T08:00:00-05:00", date(2026, 11, 20), id="normal-premarket"),
        pytest.param("2026-11-27T12:59:00-05:00", date(2026, 11, 25), id="boundary-before-half-day-close"),
        pytest.param("2026-11-27T13:00:00-05:00", date(2026, 11, 27), id="boundary-half-day-close"),
    ],
)
def test_normal_latest_complete_session_uses_actual_close(at: str, expected: date) -> None:
    """【正常值】盘前与半日市使用正式收盘时刻判断完整交易日。"""
    actual = latest_complete_session(datetime.fromisoformat(at))

    assert actual == expected


def test_validation_cloud_dsn_cannot_override_discrete_keys() -> None:
    """【参数校验】旧DSN与新离散配置同时存在时拒绝，不静默选择凭据。"""
    settings = MarketSourceSettings(
        _env_file=None,
        enabled=True,
        host="example.invalid",
        database="market",
        user="reader",
        password="synthetic",
        dsn="synthetic-legacy-dsn",
    )

    with pytest.raises(ValueError, match="PG_DSN"):
        settings.database_url()


def test_validation_disabled_source_never_uses_research_identity() -> None:
    """【参数校验】缺少行情配置时拒绝，不回退到研究库或本机账号。"""
    settings = MarketSourceSettings(_env_file=None, enabled=False)

    with pytest.raises(ValueError, match="行情连接缺少"):
        settings.database_url()
