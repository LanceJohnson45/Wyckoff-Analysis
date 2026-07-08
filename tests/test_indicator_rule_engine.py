# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd

from core.indicator_rule_engine import IndicatorRuleEngine, load_indicator_definition


def _make_ohlcv(closes: list[float], opens: list[float] | None = None) -> pd.DataFrame:
    opens = opens or closes
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=len(closes), freq="B"),
            "open": opens,
            "high": [max(o, c) * 1.01 for o, c in zip(opens, closes)],
            "low": [min(o, c) * 0.99 for o, c in zip(opens, closes)],
            "close": closes,
            "volume": [1000] * len(closes),
        }
    )


def test_before_rule_requires_background_to_happen_before_event():
    engine = IndicatorRuleEngine()
    spec = {
        "name": "before_test",
        "timeframe": "1d",
        "required_fields": ["open", "close"],
        "params": {
            "ma_window": 2,
            "background_lookback": 3,
            "background_min_count": 2,
        },
        "series": [
            {
                "id": "ma_ref",
                "op": "sma",
                "field": "close",
                "window": "$params.ma_window",
                "description": "2日均线",
            }
        ],
        "events": [
            {
                "id": "price_cross_ma",
                "op": "cross_up",
                "left": "close",
                "right": "ma_ref",
                "description": "收盘价上穿2日均线",
            }
        ],
        "rules": {
            "latest_match": {
                "op": "and",
                "conditions": [
                    {
                        "op": "before",
                        "condition": {
                            "op": "compare",
                            "left": "open",
                            "operator": ">",
                            "right": "close",
                        },
                        "event": "price_cross_ma",
                        "window": "$params.background_lookback",
                        "min_count": "$params.background_min_count",
                    },
                    {"op": "condition", "event": "price_cross_ma"},
                ],
            }
        },
    }

    passing_df = _make_ohlcv(
        closes=[10.0, 9.0, 8.0, 10.0],
        opens=[10.5, 9.5, 8.5, 9.5],
    )
    passing = engine.evaluate(passing_df, spec)
    assert passing.latest_match is True

    failing_df = _make_ohlcv(
        closes=[10.0, 10.0, 8.0, 10.0],
        opens=[9.5, 9.5, 8.5, 9.5],
    )
    failing = engine.evaluate(failing_df, spec)
    assert failing.latest_match is False


def test_jiatu_rule_matches_on_completion_day_and_expires_after_lookback():
    engine = IndicatorRuleEngine()
    spec = load_indicator_definition("/Volumes/E/github/Wyckoff-Analysis/query/001_jiatu.json")

    completion_day_closes = (
        [20.0] * 20
        + [19.0] * 5
        + [18.0] * 5
        + [18.2, 18.4, 18.6, 18.8, 19.0, 19.2, 19.4, 19.6, 19.8, 20.0, 20.2, 20.4, 20.6]
    )
    completion_df = _make_ohlcv(completion_day_closes)
    completion = engine.evaluate(completion_df, spec)

    assert completion.latest_match is True
    assert pd.Timestamp(completion.signal_date) == pd.Timestamp("2024-02-22")
    event_days = {
        event_id: completion_df.loc[event_series, "date"].dt.strftime("%Y-%m-%d").tolist()
        for event_id, event_series in completion.event_values.items()
    }
    assert event_days["cross_fast_mid"] == ["2024-02-15"]
    assert event_days["cross_fast_slow"] == ["2024-02-20"]
    assert event_days["cross_mid_slow"] == ["2024-02-22"]

    expired_df = _make_ohlcv(completion_day_closes + [20.8])
    expired = engine.evaluate(expired_df, spec)
    assert expired.latest_match is False


def test_missing_required_field_returns_non_match():
    engine = IndicatorRuleEngine()
    spec = {
        "name": "missing_field_test",
        "timeframe": "1d",
        "required_fields": ["volume"],
        "params": {},
        "series": [],
        "events": [],
        "rules": {"latest_match": {"op": "compare", "left": 1, "operator": "==", "right": 1}},
    }
    df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=3, freq="B"), "close": [1, 2, 3]})

    result = engine.evaluate(df, spec)

    assert result.latest_match is False
    assert result.required_fields_ok is False
    assert result.missing_fields == ("volume",)


def test_sub_series_op_supports_macd_style_spread():
    engine = IndicatorRuleEngine()
    spec = {
        "name": "sub_series_test",
        "timeframe": "1d",
        "required_fields": ["close"],
        "params": {},
        "series": [
            {
                "id": "close_ref",
                "op": "ref",
                "field": "close",
                "window": 1,
                "description": "前一日收盘价",
            },
            {
                "id": "close_delta",
                "op": "sub",
                "field": "close",
                "field2": "close_ref",
                "window": 0,
                "description": "收盘价差",
            },
        ],
        "events": [],
        "rules": {
            "latest_match": {
                "op": "compare",
                "left": "close_delta",
                "operator": ">",
                "right": 0,
            }
        },
    }
    df = _make_ohlcv([10.0, 11.0, 12.0])

    result = engine.evaluate(df, spec)

    assert result.latest_match is True
    assert result.series_values["close_delta"].iloc[-1] == 1.0


def test_mul_series_op_supports_scalar_multiplier():
    engine = IndicatorRuleEngine()
    spec = {
        "name": "mul_series_test",
        "timeframe": "1d",
        "required_fields": ["volume"],
        "params": {},
        "series": [
            {
                "id": "volume_ma",
                "op": "sma",
                "field": "volume",
                "window": 2,
                "description": "两日均量",
            },
            {
                "id": "double_volume_ma",
                "op": "mul",
                "field": "volume_ma",
                "field2": 2,
                "window": 0,
                "description": "两倍均量",
            },
        ],
        "events": [],
        "rules": {
            "latest_match": {
                "op": "compare",
                "left": "double_volume_ma",
                "operator": ">",
                "right": "volume_ma",
            }
        },
    }
    df = _make_ohlcv([10.0, 11.0, 12.0])
    df["volume"] = [100.0, 120.0, 140.0]

    result = engine.evaluate(df, spec)

    assert result.latest_match is True
    assert result.series_values["double_volume_ma"].iloc[-1] == 260.0


def test_consecutive_rule_accepts_legacy_days_alias():
    engine = IndicatorRuleEngine()
    spec = {
        "name": "legacy_consecutive_days_test",
        "timeframe": "1d",
        "required_fields": ["close"],
        "params": {"hold_days": 3},
        "series": [],
        "events": [],
        "rules": {
            "latest_match": {
                "op": "consecutive",
                "days": "$params.hold_days",
                "condition": {
                    "op": "compare",
                    "left": "close",
                    "operator": ">",
                    "right": 10,
                },
            }
        },
    }
    df = _make_ohlcv([9.8, 10.2, 10.5, 10.9])

    result = engine.evaluate(df, spec)

    assert result.latest_match is True


def test_symbol_context_reuses_series_and_event_calculations():
    class CountingIndicatorRuleEngine(IndicatorRuleEngine):
        def __init__(self):
            self.series_compute_count = 0
            self.event_compute_count = 0

        def _compute_series(self, op, field, window, series_map, params):
            self.series_compute_count += 1
            return super()._compute_series(op, field, window, series_map, params)

        def _compute_event_series(self, op, left, right):
            self.event_compute_count += 1
            return super()._compute_event_series(op, left, right)

    engine = CountingIndicatorRuleEngine()
    spec = {
        "name": "cache_test",
        "timeframe": "1d",
        "required_fields": ["close"],
        "params": {"ma_window": 2},
        "series": [
            {
                "id": "close_ma",
                "op": "sma",
                "field": "close",
                "window": "$params.ma_window",
                "description": "两日均线",
            }
        ],
        "events": [
            {
                "id": "cross_close_ma",
                "op": "cross_up",
                "left": "close",
                "right": "close_ma",
                "description": "收盘价上穿两日均线",
            }
        ],
        "rules": {
            "latest_match": {
                "op": "condition",
                "event": "cross_close_ma",
            }
        },
    }
    df = _make_ohlcv([10.0, 9.8, 10.2, 10.6])
    context = engine.build_symbol_context(df)

    first = engine.evaluate(df, spec, context=context)
    second = engine.evaluate(df, spec, context=context)

    assert first.latest_match is False
    assert second.latest_match is False
    assert engine.series_compute_count == 1
    assert engine.event_compute_count == 1
