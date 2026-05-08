# -*- coding: utf-8 -*-
"""
回测引擎 — 公共 API 转发层。

将 scripts/backtest_runner.py 中被其他模块引用的函数集中 re-export，
使消费者从 core/ 导入而非直接从 scripts/ 导入，保持分层干净。
"""
from scripts.backtest_runner import (  # noqa: F401
    _build_daily_nav as build_daily_nav,
    _calc_calmar_ratio as calc_calmar_ratio,
    _calc_cvar95_pct as calc_cvar95_pct,
    _calc_information_ratio as calc_information_ratio,
    _calc_max_drawdown_pct as calc_max_drawdown_pct,
    _calc_portfolio_metrics as calc_portfolio_metrics,
    _calc_sharpe_ratio as calc_sharpe_ratio,
    _fmt_metric as fmt_metric,
    _parse_date as parse_date,
    run_backtest,
)

__all__ = [
    "build_daily_nav",
    "calc_calmar_ratio",
    "calc_cvar95_pct",
    "calc_information_ratio",
    "calc_max_drawdown_pct",
    "calc_portfolio_metrics",
    "calc_sharpe_ratio",
    "fmt_metric",
    "parse_date",
    "run_backtest",
]
