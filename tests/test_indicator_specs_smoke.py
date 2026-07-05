# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from core.indicator_rule_engine import IndicatorRuleEngine, load_indicator_definition


QUERY_DIR = Path("/Volumes/E/github/Wyckoff-Analysis/query")
BASE_FIELDS = {"open", "high", "low", "close", "volume"}


def _make_smoke_ohlcv(size: int = 160) -> pd.DataFrame:
    closes: list[float] = []
    for idx in range(size):
        if idx < 40:
            closes.append(30.0 - idx * 0.2)
        elif idx < 80:
            closes.append(22.0 + (idx - 40) * 0.12)
        elif idx < 120:
            closes.append(26.8 - (idx - 80) * 0.08)
        else:
            closes.append(23.6 + (idx - 120) * 0.18)
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=size, freq="B"),
            "open": [c * 0.998 for c in closes],
            "high": [c * 1.01 for c in closes],
            "low": [c * 0.99 for c in closes],
            "close": closes,
            "volume": [1_000_000 + idx * 1000 for idx in range(size)],
        }
    )


def _indicator_paths() -> list[Path]:
    return sorted(
        path
        for path in QUERY_DIR.glob("*.json")
        if path.name not in {"extraction_progress.json"}
    )


@pytest.mark.parametrize("spec_path", _indicator_paths(), ids=lambda path: path.stem)
def test_indicator_specs_load_and_evaluate_without_runtime_errors(spec_path: Path):
    spec = load_indicator_definition(spec_path)
    engine = IndicatorRuleEngine()
    df = _make_smoke_ohlcv()

    result = engine.evaluate(df, spec)

    assert result.name == spec["name"]
    assert result.timeframe == spec["timeframe"]
    assert isinstance(result.latest_match, bool)
    assert isinstance(result.event_values, dict)
    assert isinstance(result.series_values, dict)


@pytest.mark.parametrize("spec_path", _indicator_paths(), ids=lambda path: path.stem)
def test_computable_indicator_required_fields_stay_within_daily_ohlcv(spec_path: Path):
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if not spec.get("computable_from_daily_ocvhl", False):
        pytest.skip("Non-computable indicators are documented separately")

    required_fields = set(spec.get("required_fields", []))
    assert required_fields.issubset(BASE_FIELDS)
