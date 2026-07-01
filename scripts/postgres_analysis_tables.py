from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    name: str
    conflict_columns: tuple[str, ...]
    order_columns: tuple[str, ...] = ()
    json_columns: tuple[str, ...] = ()
    reset_sequence_column: str | None = None


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="stock_hist_cache",
        conflict_columns=("symbol", "adjust", "date"),
        order_columns=("symbol", "adjust", "date"),
    ),
    TableSpec(
        name="market_signal_daily",
        conflict_columns=("trade_date", "market"),
        order_columns=("trade_date", "market"),
        json_columns=("premarket_reasons", "source_jobs"),
    ),
    TableSpec(
        name="recommendation_tracking",
        conflict_columns=("market", "symbol", "recommend_date"),
        order_columns=("recommend_date", "market", "symbol"),
        reset_sequence_column="id",
    ),
    TableSpec(
        name="signal_pending",
        conflict_columns=("id",),
        order_columns=("id",),
        reset_sequence_column="id",
    ),
    TableSpec(
        name="index_constituents_snapshot",
        conflict_columns=("market", "index_code"),
        order_columns=("market", "index_code"),
        json_columns=("raw_payload",),
    ),
    TableSpec(
        name="portfolios",
        conflict_columns=("portfolio_id",),
        order_columns=("portfolio_id",),
    ),
    TableSpec(
        name="portfolio_positions",
        conflict_columns=("portfolio_id", "market", "code"),
        order_columns=("portfolio_id", "market", "code"),
    ),
    TableSpec(
        name="trade_orders",
        conflict_columns=("id",),
        order_columns=("id",),
        reset_sequence_column="id",
    ),
    TableSpec(
        name="daily_nav",
        conflict_columns=("portfolio_id", "trade_date"),
        order_columns=("portfolio_id", "trade_date"),
    ),
)


TABLE_SPEC_MAP = {spec.name: spec for spec in TABLE_SPECS}
