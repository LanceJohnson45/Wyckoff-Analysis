"""批量回补应只接收指定日期和标的的有效前复权日线。"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

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
