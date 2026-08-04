# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest


def _valid_hist(rows: int = 220) -> pd.DataFrame:
    dates = pd.date_range("2025-09-01", periods=rows, freq="B")
    close = pd.Series(range(100, 100 + rows), dtype=float)
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": 1000.0,
            "amount": close * 1000.0,
            "pct_chg": close.pct_change() * 100.0,
        }
    )


def test_yfinance_all_null_ohlc_falls_back_to_tickflow(monkeypatch: pytest.MonkeyPatch):
    from integrations import data_source as ds

    monkeypatch.setenv("TICKFLOW_API_KEY", "dummy")
    monkeypatch.delenv("TIKFLOW_API_KEY", raising=False)
    monkeypatch.setattr(
        ds,
        "_fetch_stock_yfinance",
        lambda *args, **kwargs: pd.DataFrame(
            {
                "日期": ["2026-08-03"],
                "开盘": [None],
                "最高": [None],
                "最低": [None],
                "收盘": [None],
                "成交量": [100.0],
            }
        ),
    )
    monkeypatch.setattr(ds, "_fetch_stock_tickflow_global", lambda *args, **kwargs: _valid_hist(1))

    result = ds.fetch_stock_hist("ZS", date(2026, 8, 3), date(2026, 8, 3), market="us")

    assert result.attrs["source"] == "tickflow"
    assert result["close"].notna().all()


def test_failed_cache_write_does_not_advance_cache_meta(monkeypatch: pytest.MonkeyPatch):
    from integrations import stock_hist_repository as repo

    meta_writes = []
    monkeypatch.setattr(repo, "get_cache_meta", lambda *args, **kwargs: None)
    monkeypatch.setattr(repo, "_fetch_gap", lambda *args, **kwargs: (_valid_hist(1), "tickflow"))
    monkeypatch.setattr(repo, "upsert_cache_data", lambda *args, **kwargs: False)
    monkeypatch.setattr(repo, "upsert_cache_meta", lambda *args, **kwargs: meta_writes.append(args))

    with pytest.raises(RuntimeError, match="cache_upsert failed"):
        repo.get_stock_hist(
            "ZS",
            date(2026, 8, 3),
            date(2026, 8, 3),
            market="us",
            context="background",
        )

    assert meta_writes == []


def test_small_breadth_sample_cannot_trigger_crash(monkeypatch: pytest.MonkeyPatch):
    from scripts import wyckoff_funnel as funnel
    from core.wyckoff_engine import FunnelConfig

    monkeypatch.setattr(funnel, "_resolve_funnel_market", lambda: "us")
    benchmark = _valid_hist()
    context = funnel._analyze_benchmark_and_tune_cfg(
        benchmark,
        benchmark,
        FunnelConfig.for_market("us"),
        breadth={"ratio_pct": 0.0, "prev_ratio_pct": 100.0, "delta_pct": -100.0, "sample_size": 1},
    )

    assert context["regime"] != "CRASH"
    assert context["panic_triggered"] is False


def test_prewarm_repairs_full_window_when_cache_has_only_a_few_rows(monkeypatch: pytest.MonkeyPatch):
    import scripts.funnel_prewarm as prewarm

    expected = [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3), date(2026, 7, 6)]
    window = type("Window", (), {"start_trade_date": expected[0], "end_trade_date": expected[-1]})()
    calls = []
    load_calls = 0

    monkeypatch.setattr(prewarm, "_job_end_calendar_day", lambda: expected[-1])
    monkeypatch.setattr(prewarm, "_resolve_us_window", lambda **kwargs: window)
    monkeypatch.setattr(prewarm, "_expected_trade_dates", lambda *args, **kwargs: expected)
    monkeypatch.setattr(prewarm, "_RECENT_GAP_MAX_AGE_DAYS", 45)

    def fake_load_cached_dates(*args, **kwargs):
        nonlocal load_calls
        load_calls += 1
        return [expected[0]] if load_calls == 1 else expected

    monkeypatch.setattr(prewarm, "load_cached_dates", fake_load_cached_dates)
    monkeypatch.setattr(
        prewarm,
        "fetch_stock_hist",
        lambda **kwargs: calls.append((kwargs["start"], kwargs["end"])) or _valid_hist(1),
    )
    monkeypatch.setattr(prewarm, "upsert_cache_data", lambda **kwargs: True)

    result = prewarm._prefetch_one("ZS", "us", 4)

    assert result[1] == "gap_repaired"
    assert calls == [(date(2026, 7, 2), date(2026, 7, 6))]
