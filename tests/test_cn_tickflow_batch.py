"""批量回补应只接收指定日期和标的的有效前复权日线。"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from scripts.cn_tickflow_batch import _records


def test_batch_rows_keep_only_requested_day_and_valid_prices() -> None:
    """跳过重复日期、错误 OHLC 及停牌零量，同时计算前收盘口径。"""
    tz = ZoneInfo("Asia/Shanghai")
    now = datetime(2026, 11, 17, 18, tzinfo=tz)
    days = [date(2026, 11, day) for day in (11, 12, 13, 16, 16, 17)]
    data = {
        "timestamp": [int(datetime.combine(day, datetime.min.time(), tzinfo=tz).timestamp() * 1000) for day in days],
        "open": [100] * 6,
        "high": [103, 103, 99, 103, 103, 103],
        "low": [98] * 6,
        "close": [102] * 6,
        "volume": [1000, 0, 1000, 1000, 1000, 1000],
        "amount": [102000] * 6,
        "prev_close": [100] * 6,
    }
    rows = _records({"600519.SH": data, "600000.SH": data}, {"600519.SH": set(days[1:])}, now)
    assert [(row["symbol"], row["date"], row["adjust"]) for row in rows] == [("600519", days[-1], "qfq")]
    assert abs(rows[0]["pct_chg"] - 2) < 1e-9
    assert rows[0]["amplitude"] == 5
    assert rows[0]["updated_at"] == now


def test_historical_suspension_uses_verified_continuous_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """补漏只跳过有日期证据的停牌日，正常缺口仍需核验。"""
    from time import monotonic
    from scripts import funnel_prewarm as prewarm

    first, second, third, end = [date(2026, 11, day) for day in (12, 13, 16, 17)]
    queried = []

    def lookup(function, day, *, timeout):
        queried.append(day)
        return {"600519": prewarm.SuspensionPeriod(second, "停牌证据")} if day == third else {}

    monkeypatch.setattr(prewarm, "bounded_call", lookup)
    result = prewarm._confirm_historical_suspensions(
        [first, second, third, end], end, {"600519": {first, end}, "000001": {first, end}}, {}, deadline=monotonic() + 10,
    )
    assert result == {"600519": {second, third}}
    assert queried == [third, second]


def test_documented_suspension_is_bounded_and_works_without_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    """历史停牌以已披露区间为准，复牌后日线仍保留在核验范围。"""
    from time import monotonic
    from integrations.market_data_policy import announced_suspension_days, load_security_overrides
    from scripts import funnel_prewarm as prewarm

    prior, stopped, resumed, target = [date(2026, 11, day) for day in (2, 9, 10, 17)]
    days = [prior, stopped, resumed, target]
    known = announced_suspension_days(load_security_overrides()[("cn", "002623")], days)
    assert known == {stopped}

    def unexpected_lookup(*args, **kwargs):
        pytest.fail("公告已确认的日期不应再次请求停牌接口")

    monkeypatch.setattr(prewarm, "bounded_call", unexpected_lookup)
    assert prewarm._confirm_historical_suspensions(
        days, target, {"002623": {prior, resumed, target}}, {},
        deadline=monotonic() + 10, announced={"002623": known},
    ) == {"002623": {stopped}}
