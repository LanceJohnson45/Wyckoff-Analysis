# -*- coding: utf-8 -*-
"""
Wyckoff 漏斗管线 — 公共 API 转发层。

将 scripts/wyckoff_funnel.py 中被其他模块引用的函数集中 re-export，
使消费者从 core/ 导入而非直接从 scripts/ 导入，保持分层干净。
"""
from scripts.wyckoff_funnel import (  # noqa: F401
    TRIGGER_LABELS,
    _analyze_benchmark_and_tune_cfg as analyze_benchmark_and_tune_cfg,
    _calc_market_breadth as calc_market_breadth,
    _rank_l3_candidates as rank_l3_candidates,
    run as run_funnel,
    run_funnel_job,
)

__all__ = [
    "TRIGGER_LABELS",
    "analyze_benchmark_and_tune_cfg",
    "calc_market_breadth",
    "rank_l3_candidates",
    "run_funnel",
    "run_funnel_job",
]
