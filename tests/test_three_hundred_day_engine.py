# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from core.three_hundred_day_engine import scan_three_hundred_day, scan_three_hundred_day_lazy


def _make_df(close_value: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=40, freq="B"),
            "open": [close_value] * 40,
            "high": [close_value * 1.01] * 40,
            "low": [close_value * 0.99] * 40,
            "close": [close_value] * 40,
            "volume": [1_000_000] * 40,
        }
    )


def test_scan_three_hundred_day_writes_daily_report(tmp_path: Path):
    query_dir = tmp_path / "query"
    output_dir = tmp_path / "out"
    query_dir.mkdir()

    matching_spec = {
        "name": "站上10元",
        "status": "structured",
        "timeframe": "1d",
        "source": "收盘价大于10元。",
        "source_summary": "最新收盘价高于10元。",
        "comment": "这是一个测试指标。",
        "computable_from_daily_ocvhl": True,
        "required_fields": ["close"],
        "warmup_bars": 5,
        "indicator_type": "other",
        "params": {},
        "series": [],
        "events": [],
        "rules": {
            "latest_match": {
                "op": "compare",
                "left": "close",
                "operator": ">",
                "right": 10,
            }
        },
        "calculation_steps": [],
        "signal_day_definition": "当日",
        "manual_review_needed": [],
        "implementation_notes": [],
        "explain": "测试指标",
    }
    non_matching_spec = {
        **matching_spec,
        "name": "站上100元",
        "source_summary": "最新收盘价高于100元。",
        "rules": {
            "latest_match": {
                "op": "compare",
                "left": "close",
                "operator": ">",
                "right": 100,
            }
        },
    }
    non_computable_spec = {
        **matching_spec,
        "name": "分时专用",
        "computable_from_daily_ocvhl": False,
    }

    (query_dir / "match.json").write_text(
        json.dumps(matching_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (query_dir / "no_match.json").write_text(
        json.dumps(non_matching_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (query_dir / "intraday_only.json").write_text(
        json.dumps(non_computable_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    result = scan_three_hundred_day(
        df_map={"000001": _make_df(12.0), "000002": _make_df(8.0)},
        name_map={"000001": "平安银行", "000002": "万科A"},
        trade_date="2026-07-07",
        market="cn",
        query_dir=query_dir,
        output_dir=output_dir,
    )

    assert result.scanned_symbols == 2
    assert result.matched_symbols == 1
    assert result.matched_indicator_total == 1
    assert result.symbols[0].code == "000001"
    assert result.symbols[0].name == "平安银行"
    assert result.symbols[0].indicators[0].indicator == "站上10元"

    report_path = output_dir / "2026-07-07_300day记录.txt"
    assert result.output_path == str(report_path)
    content = report_path.read_text(encoding="utf-8")
    assert "A股 300day 扫描结果" in content
    assert "A股 个股：000001 平安银行 今日达标：站上10元 原因：" in content
    assert "000002" not in content


def test_scan_three_hundred_day_lazy_fetches_by_indicator_requirements(tmp_path: Path):
    query_dir = tmp_path / "query"
    output_dir = tmp_path / "out"
    query_dir.mkdir()

    fast_spec = {
        "name": "五日站上10元",
        "status": "structured",
        "timeframe": "1d",
        "source": "五日后收盘价大于10元。",
        "source_summary": "短窗口测试。",
        "comment": "",
        "computable_from_daily_ocvhl": True,
        "required_fields": ["close"],
        "warmup_bars": 5,
        "indicator_type": "other",
        "params": {},
        "series": [],
        "events": [],
        "rules": {"latest_match": {"op": "compare", "left": "close", "operator": ">", "right": 10}},
        "calculation_steps": [],
        "signal_day_definition": "当日",
        "manual_review_needed": [],
        "implementation_notes": [],
        "explain": "短窗口",
    }
    slow_spec = {
        **fast_spec,
        "name": "二十日站上20元",
        "warmup_bars": 20,
        "source_summary": "长窗口测试。",
        "rules": {"latest_match": {"op": "compare", "left": "close", "operator": ">", "right": 20}},
    }
    (query_dir / "fast.json").write_text(
        json.dumps(fast_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (query_dir / "slow.json").write_text(
        json.dumps(slow_spec, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    calls: list[int] = []

    def history_loader(symbol: str, bars: int):
        calls.append(int(bars))
        close_value = 12.0 if bars <= 25 else 25.0
        return _make_df(close_value).tail(bars).reset_index(drop=True)

    result = scan_three_hundred_day_lazy(
        symbols=["000001"],
        name_map={"000001": "平安银行"},
        trade_date="2026-07-07",
        history_loader=history_loader,
        market="cn",
        query_dir=query_dir,
        output_dir=output_dir,
    )

    assert result.scanned_symbols == 1
    assert result.matched_symbols == 1
    assert result.matched_indicator_total == 2
    assert calls == [25, 40]
