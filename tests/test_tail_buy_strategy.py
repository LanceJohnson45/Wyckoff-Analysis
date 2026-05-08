# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime

import pandas as pd

from core.tail_buy_strategy import (
    DECISION_BUY,
    DECISION_SKIP,
    DECISION_WATCH,
    TailBuyCandidate,
    compute_tail_features,
    evaluate_rule_decision,
    merge_rule_and_llm,
    pick_tail_candidates,
)


def _make_intraday_df(
    *,
    start: float,
    end: float,
    bars: int = 180,
    tail_boost: float = 0.0,
    tail_volume_mult: float = 1.0,
) -> pd.DataFrame:
    idx = pd.date_range(
        start=datetime(2026, 4, 21, 9, 30),
        periods=bars,
        freq="1min",
        tz="Asia/Shanghai",
    )
    base = pd.Series([start + (end - start) * i / max(bars - 1, 1) for i in range(bars)])
    if tail_boost != 0.0:
        tail_n = min(30, bars)
        tail_delta = pd.Series(
            [tail_boost * (i + 1) / tail_n for i in range(tail_n)],
            index=base.index[-tail_n:],
        )
        base.iloc[-tail_n:] = base.iloc[-tail_n:].to_numpy() + tail_delta.to_numpy()
    close = base
    open_ = close.shift(1).fillna(close.iloc[0]) * 0.999
    high = close * 1.003
    low = close * 0.997
    volume = pd.Series([1200.0] * bars)
    tail_n = min(30, bars)
    volume.iloc[-tail_n:] = volume.iloc[-tail_n:] * tail_volume_mult
    amount = close * volume
    return pd.DataFrame(
        {
            "datetime": idx,
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": volume.values,
            "amount": amount.values,
        }
    )


def test_pick_tail_candidates_filters_prev_trade_day_and_status():
    rows = [
        {
            "code": 301090,
            "name": "华润材料",
            "signal_type": "spring",
            "signal_score": 3.2,
            "status": "pending",
            "signal_date": "2026-04-20",
        },
        {
            "code": 301090,
            "name": "华润材料",
            "signal_type": "spring",
            "signal_score": 2.9,
            "status": "confirmed",
            "signal_date": "2026-04-20",
        },
        {
            "code": "600000",
            "name": "浦发银行",
            "signal_type": "sos",
            "signal_score": 1.5,
            "status": "expired",
            "signal_date": "2026-04-20",
        },
        {
            "code": "000001",
            "name": "平安银行",
            "signal_type": "lps",
            "signal_score": 2.1,
            "status": "pending",
            "signal_date": "2026-04-18",
        },
        {
            "code": "002217",
            "name": "合力泰",
            "signal_type": "sos",
            "signal_score": 5.1,
            "status": "pending",
            "signal_date": "2026-04-20",
        },
    ]
    got = pick_tail_candidates(rows, target_signal_date="2026-04-20")
    assert [x.code for x in got] == ["301090", "002217"]
    assert got[0].status == "confirmed"


def test_evaluate_rule_decision_buy_and_skip_split():
    strong = TailBuyCandidate(
        code="301090",
        name="华润材料",
        signal_date="2026-04-20",
        status="confirmed",
        signal_type="spring",
        signal_score=6.0,
    )
    weak = TailBuyCandidate(
        code="600000",
        name="浦发银行",
        signal_date="2026-04-20",
        status="pending",
        signal_type="sos",
        signal_score=1.0,
    )
    strong_df = _make_intraday_df(start=10.0, end=10.9, tail_boost=0.8, tail_volume_mult=2.0)
    weak_df = _make_intraday_df(start=10.0, end=9.6, tail_boost=-0.2, tail_volume_mult=0.6)

    strong_out = evaluate_rule_decision(strong, strong_df, style="hybrid")
    weak_out = evaluate_rule_decision(weak, weak_df, style="hybrid")

    assert strong_out.rule_decision in {DECISION_BUY, DECISION_WATCH}
    assert strong_out.rule_score > weak_out.rule_score
    assert weak_out.rule_decision == DECISION_SKIP


def test_merge_rule_and_llm_keeps_non_top_symbols_on_rule_decision():
    c1 = TailBuyCandidate(
        code="301090",
        name="华润材料",
        signal_date="2026-04-20",
        status="confirmed",
        signal_type="spring",
        signal_score=5.0,
        rule_score=80.0,
        rule_decision=DECISION_BUY,
        final_decision=DECISION_BUY,
    )
    c2 = TailBuyCandidate(
        code="002217",
        name="合力泰",
        signal_date="2026-04-20",
        status="pending",
        signal_type="sos",
        signal_score=4.0,
        rule_score=60.0,
        rule_decision=DECISION_WATCH,
        final_decision=DECISION_WATCH,
    )
    c3 = TailBuyCandidate(
        code="600000",
        name="浦发银行",
        signal_date="2026-04-20",
        status="pending",
        signal_type="sos",
        signal_score=2.0,
        rule_score=35.0,
        rule_decision=DECISION_SKIP,
        final_decision=DECISION_SKIP,
    )

    llm_map = {
        "002217": {"decision": DECISION_BUY, "reason": "尾盘再加速", "confidence": 0.76, "model_used": "nvidia-kimi:moonshot"},
        "301090": {"decision": DECISION_WATCH, "reason": "高位波动扩大", "confidence": 0.64, "model_used": "gemini:flash"},
    }
    merged = merge_rule_and_llm([c1, c2, c3], llm_map)
    by_code = {x.code: x for x in merged}

    assert by_code["002217"].final_decision == DECISION_BUY
    assert by_code["301090"].final_decision == DECISION_WATCH
    assert by_code["600000"].final_decision == DECISION_SKIP
    assert by_code["600000"].llm_decision is None
    assert by_code["002217"].llm_model_used.startswith("nvidia-kimi")


def test_compute_tail_features_handles_volume_lot_unit_for_vwap():
    df = _make_intraday_df(start=10.0, end=10.6, tail_boost=0.3, tail_volume_mult=1.3)
    # 模拟 TickFlow: volume 为“手”，amount 为“元”（需 /100 才接近真实价格）
    df["amount"] = df["close"] * df["volume"] * 100.0
    feats = compute_tail_features(df)
    assert feats["bars"] >= 60
    assert feats["vwap_volume_scale"] == 100.0
    assert 8.0 < feats["vwap"] < 20.0
    assert feats["dist_vwap_pct"] > -20.0
