"""Martin 日线形态、数据边界与无前视验证。"""

from datetime import date

import pandas as pd
import pytest

from core.martin_luk import LukParameters, analyze_daily, normalize_daily_frame


@pytest.fixture
def rising_daily() -> pd.DataFrame:
    """【正常值】构造已知缓慢上升的日线，不依赖外部行情。"""
    close = pd.Series([40 + i * 0.1 for i in range(120)])
    return pd.DataFrame({
        "date": pd.bdate_range("2025-01-01", periods=120),
        "open": close, "high": close + 0.6, "low": close - 0.6,
        "close": close, "volume": 1000,
    })


def test_normal_pullback_has_explainable_reference_levels(rising_daily: pd.DataFrame) -> None:
    """【正常值】趋势回调给出当时已知的高点与失效参考。"""
    frame = rising_daily.copy()
    frame.loc[119, ["open", "high", "low", "close"]] = [51, 51.5, 50.5, 51]
    cutoff = frame.iloc[-1]["date"].date()

    observation = analyze_daily(frame, symbol="TEST", market="us", as_of=cutoff)

    assert observation.tags == ("强势回调",)
    assert observation.trigger == 51.5
    assert observation.invalidation == 50.5
    assert "日线候选不代表分钟入场" in observation.next_step


def test_normal_uninterrupted_rally_is_not_a_pullback(rising_daily: pd.DataFrame) -> None:
    """【正常值】每天收盘与低点均抬高的趋势不冒充发生过回调。"""
    cutoff = rising_daily.iloc[-1]["date"].date()

    result = analyze_daily(rising_daily, symbol="TEST", market="us", as_of=cutoff)

    assert "强势回调" not in result.tags


def test_normal_contraction_then_breakout_are_distinct(rising_daily: pd.DataFrame) -> None:
    """【正常值】同一整理在突破前后有不同状态，且突破参考不包含当日高。"""
    frame = rising_daily.copy()
    for i, price in zip([117, 118, 119], [51.7, 51.75, 51.8]):
        frame.loc[i, ["open", "high", "low", "close", "volume"]] = [price, price + 0.15, price - 0.15, price, 600]
    cutoff = frame.iloc[-1]["date"].date()

    waiting = analyze_daily(frame, symbol="TEST", market="us", as_of=cutoff)
    frame.loc[120] = [pd.Timestamp("2025-06-18"), 52.4, 52.6, 52.2, 52.5, 1200]
    breakout = analyze_daily(frame, symbol="TEST", market="us", as_of=date(2025, 6, 18))

    assert "收紧待突破" in waiting.tags
    assert "收紧突破" not in waiting.tags
    assert "收紧突破" in breakout.tags
    assert breakout.trigger == pytest.approx(52.2)
    assert breakout.trigger < 52.6


def test_normal_undercut_reclaim_is_not_dip_buying(rising_daily: pd.DataFrame) -> None:
    """【正常值】仅下穿旧低不会产生U&R，收盘站回才产生。"""
    frame = rising_daily.copy()
    old_low = frame["low"].iloc[-21:-1].min()
    frame.loc[119, ["open", "high", "low", "close"]] = [old_low - 0.1, old_low + 0.3, old_low - 0.5, old_low - 0.2]
    cutoff = frame.iloc[-1]["date"].date()

    unreclaimed = analyze_daily(frame, symbol="TEST", market="us", as_of=cutoff)
    frame.loc[119, "close"] = old_low + 0.2
    reclaimed = analyze_daily(frame, symbol="TEST", market="us", as_of=cutoff)

    assert "U&R 收复" not in unreclaimed.tags
    assert "U&R 收复" in reclaimed.tags
    assert reclaimed.invalidation == pytest.approx(old_low - 0.5)
    assert any("非完整多头" in w for w in reclaimed.warnings)


def test_boundary_future_bars_do_not_change_past_observation(rising_daily: pd.DataFrame) -> None:
    """【边界值】后续暴涨不改变既有日期的标签、参考位或指标。"""
    cutoff = rising_daily.iloc[-2]["date"].date()
    altered = rising_daily.copy()
    altered.loc[119, ["open", "high", "low", "close"]] = [1000, 1001, 999, 1000]

    before = analyze_daily(rising_daily.iloc[:-1], symbol="TEST", market="us", as_of=cutoff)
    after = analyze_daily(altered, symbol="TEST", market="us", as_of=cutoff)

    assert before.summary() == after.summary()
    pd.testing.assert_frame_equal(before.frame, after.frame)


@pytest.mark.parametrize("fault", ["duplicate", "missing_volume", "infinite", "invalid_ohlc", "zero_latest", "intraday", "short"], ids=["duplicate", "missing-volume", "infinite", "invalid-ohlc", "zero-latest", "intraday", "short"])
def test_error_bad_daily_data_cannot_generate_signals(rising_daily: pd.DataFrame, fault: str) -> None:
    """【异常值】重复日线、无效价格量和不足历史必须显式拒绝。"""
    frame = rising_daily.copy()
    if fault == "duplicate":
        frame.loc[119, "date"] = frame.loc[118, "date"]
    elif fault == "missing_volume":
        frame.loc[119, "volume"] = float("nan")
    elif fault == "infinite":
        frame.loc[119, "high"] = float("inf")
    elif fault == "invalid_ohlc":
        frame.loc[119, "low"] = 100
    elif fault == "zero_latest":
        frame.loc[119, "volume"] = 0
    elif fault == "intraday":
        frame.loc[119, "date"] += pd.Timedelta(hours=9)
    else:
        frame = frame.tail(99)

    with pytest.raises(ValueError):
        analyze_daily(frame, symbol="TEST", market="cn", as_of=date(2025, 6, 18))


def test_normal_benchmark_uses_same_dates_and_accepts_zero_index_volume(rising_daily: pd.DataFrame) -> None:
    """【正常值】基准零量允许作价格对照，不能冒充个股有效成交。"""
    benchmark = rising_daily.copy()
    benchmark["volume"] = 0
    cutoff = rising_daily.iloc[-1]["date"].date()

    result = analyze_daily(rising_daily, symbol="TEST", market="cn", as_of=cutoff, benchmark=benchmark)

    assert result.rs20_pct == pytest.approx(0)
    with pytest.raises(ValueError, match="成交量为零"):
        normalize_daily_frame(benchmark, as_of=cutoff)


def test_boundary_missing_benchmark_date_is_not_forward_filled(rising_daily: pd.DataFrame) -> None:
    """【边界值】基准日期缺失时相对强度留空并说明原因。"""
    benchmark = rising_daily.drop(index=118)
    cutoff = rising_daily.iloc[-1]["date"].date()

    result = analyze_daily(rising_daily, symbol="TEST", market="us", as_of=cutoff, benchmark=benchmark)

    assert result.rs20_pct is None
    assert any("未完全对齐" in warning for warning in result.warnings)


def test_normal_extended_price_is_not_pullback_candidate(rising_daily: pd.DataFrame) -> None:
    """【正常值】急涨延伸提示风险，不被当成普通回调候选。"""
    frame = rising_daily.copy()
    frame.loc[119, ["open", "high", "low", "close"]] = [70, 71, 69, 70]
    cutoff = frame.iloc[-1]["date"].date()

    result = analyze_daily(frame, symbol="TEST", market="us", as_of=cutoff)

    assert "延伸预警" in result.tags
    assert "强势回调" not in result.tags


@pytest.mark.parametrize("kwargs", [{"support_atr": 0}, {"extension_atr": 0.5}, {"contraction_ratio": 1}], ids=["zero-support", "inverted-distance", "no-contraction"])
def test_param_invalid_thresholds_are_rejected(kwargs: dict[str, float]) -> None:
    """【参数校验】拒绝无意义的支撑带和收缩阈值。"""
    with pytest.raises(ValueError):
        LukParameters(**kwargs)
