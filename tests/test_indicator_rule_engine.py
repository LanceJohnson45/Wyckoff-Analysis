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
    spec = load_indicator_definition("/Volumes/E/github/Wyckoff-Analysis/query/jiatu.json")

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
