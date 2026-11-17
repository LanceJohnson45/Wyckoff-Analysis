"""在 A 股逐股核验前，用 TickFlow 批量补齐近期缺失的前复权日线。"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date, datetime, time, timedelta
from math import isfinite
from pathlib import Path
from time import monotonic
from zoneinfo import ZoneInfo

import httpx
import psycopg
import structlog
from sqlalchemy import column, create_engine, func, select, table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from core.market_scope import excluded_cn_board
from integrations.market_data_policy import exclusion_applies, fetch_cn_suspensions, load_security_overrides
from integrations.postgres_base import get_postgres_dsn, postgres_configured, postgres_enabled
from integrations.tickflow_client import normalize_cn_symbol, parse_ohlcv_payload
from scripts.market_data_batches import BatchPlan, load_plan
from scripts.prewarm_workers import bounded_call

CN_TZ = ZoneInfo("Asia/Shanghai")
BATCH_SIZE = 100
MIN_REQUEST_INTERVAL = 1.1
MAX_TARGET_FALLBACK_SYMBOLS = 100
DB_WRITE_BATCH_SIZE = 1000
BAR_COLUMNS = ("open", "high", "low", "close", "volume")
CACHE = table(
    "stock_hist_cache",
    *(column(name) for name in ("symbol", "adjust", "date", *BAR_COLUMNS, "amount", "pct_chg", "amplitude", "updated_at")),
    schema="public",
)
LOGGER = structlog.get_logger()


def _cache_engine() -> Engine:
    dsn = get_postgres_dsn()
    return create_engine(
        "postgresql+psycopg://", hide_parameters=True,
        creator=lambda: psycopg.connect(dsn, connect_timeout=10, options="-c statement_timeout=60000"),
    )


def _cached_days(engine: Engine, symbols: list[str], start: date, end: date) -> dict[str, set[date]]:
    if not symbols:
        return {}
    query = select(CACHE.c.symbol, func.array_agg(CACHE.c.date)).where(
        CACHE.c.symbol.in_(symbols), CACHE.c.adjust == "qfq", CACHE.c.date.between(start, end),
    ).group_by(CACHE.c.symbol)
    with engine.connect() as connection:
        return {symbol: set(days) for symbol, days in connection.execute(query)}


def load_cn_cache_days(symbols: list[str], start: date, end: date) -> dict[str, set[date]]:
    """为冻结批次一次读取缓存日期，避免对每只股票各连一次数据库。"""
    engine = _cache_engine()
    try:
        return _cached_days(engine, symbols, start, end)
    finally:
        engine.dispose()


def _missing_days(engine: Engine, symbols: list[str], days: set[date]) -> dict[str, set[date]]:
    existing = _cached_days(engine, symbols, min(days), max(days))
    return {symbol: missing for symbol in symbols if (missing := days - existing.get(symbol, set()))}


def _optional_number(value: float) -> float | None:
    return float(value) if isfinite(value) else None


def _records(payload: dict, expected: dict[str, set[date]], now: datetime) -> list[dict]:
    rows = []
    for code, days in expected.items():
        if code not in payload:
            continue
        try:
            frame = parse_ohlcv_payload({"data": payload[code]})
        except (TypeError, ValueError) as exc:
            LOGGER.warning("cn_tickflow_invalid_payload", code=code, error_type=type(exc).__name__)
            continue
        if frame.empty:
            continue
        frame = frame.drop_duplicates(subset="date", keep=False)
        for bar in frame.itertuples(index=False):
            day = date.fromisoformat(bar.date)
            if day not in days:
                continue
            values = {name: float(getattr(bar, name)) for name in BAR_COLUMNS}
            if not all(isfinite(value) and value > 0 for value in values.values()):
                continue
            if values["low"] > min(values["open"], values["close"]) or values["high"] < max(values["open"], values["close"]):
                continue
            previous = _optional_number(bar.prev_close)
            rows.append({
                "symbol": code.partition(".")[0], "adjust": "qfq", "date": day, **values,
                "amount": _optional_number(bar.amount),
                "pct_chg": (values["close"] / previous - 1) * 100 if previous and previous > 0 else None,
                "amplitude": (values["high"] - values["low"]) / previous * 100 if previous and previous > 0 else None,
                "updated_at": now,
            })
    return rows


async def _download(missing: dict[str, set[date]], target: date) -> list[dict]:
    rows: list[dict] = []
    last_request = float("-inf")
    end = datetime.combine(target + timedelta(days=1), time.min, tzinfo=CN_TZ) - timedelta(milliseconds=1)
    now = datetime.now(CN_TZ)
    async with httpx.AsyncClient(base_url="https://free-api.tickflow.org", timeout=12) as client:
        symbols = list(missing)
        for offset in range(0, len(symbols), BATCH_SIZE):
            number = offset // BATCH_SIZE + 1
            chunk = symbols[offset:offset + BATCH_SIZE]
            expected = {normalize_cn_symbol(symbol): missing[symbol] for symbol in chunk}
            start = datetime.combine(min(min(days) for days in expected.values()), time.min, tzinfo=CN_TZ)
            params = {
                "symbols": ",".join(expected), "period": "1d", "adjust": "forward", "count": (end - start).days + 2,
                "start_time": int(start.timestamp() * 1000), "end_time": int(end.timestamp() * 1000),
            }
            body = None
            for attempt in range(2):
                await asyncio.sleep(max(0, MIN_REQUEST_INTERVAL - (monotonic() - last_request)))
                last_request = monotonic()
                try:
                    response = await client.get("/v1/klines/batch", params=params)
                    response.raise_for_status()
                    payload = response.json()
                    body = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(body, dict):
                        raise ValueError("缺少批量日线 data 字段")
                    break
                except (httpx.HTTPError, ValueError) as exc:
                    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
                    reason = status or type(exc).__name__
                    if status is not None and status < 500 and status != 429:
                        raise RuntimeError(f"TickFlow 批次 {number} 请求失败（{reason}）") from None
                    if attempt:
                        LOGGER.warning("cn_tickflow_batch_unavailable", batch=number, symbols=len(chunk), reason=reason)
                    else:
                        await asyncio.sleep(5 if status == 429 else 2)
            if not isinstance(body, dict):
                continue
            batch_rows = _records(body, expected, now)
            rows.extend(batch_rows)
            LOGGER.info("cn_tickflow_batch", batch=number, symbols=len(chunk), rows=len(batch_rows))
    return rows


def prefill(plan: BatchPlan, engine: Engine, *, recent_days: int) -> tuple[int, int]:
    """写入缺失的有效日线，返回新写行数和目标日仍待核验的股票数。"""
    target = date.fromisoformat(plan["target_date"])
    cutoff = target - timedelta(days=recent_days)
    days = {date.fromisoformat(raw) for raw in plan["trade_dates"] if not recent_days or raw >= cutoff.isoformat()}
    overrides = load_security_overrides()
    symbols = [
        symbol
        for part in plan["batches"]
        for item in part["securities"]
        if not excluded_cn_board(symbol := item["code"], "cn")
        and not exclusion_applies(overrides.get(("cn", symbol), {}), target)
    ]
    missing = _missing_days(engine, symbols, days)
    suspended = {}
    if missing:
        try:
            suspended = bounded_call(fetch_cn_suspensions, target, timeout=15)
        except (RuntimeError, TimeoutError) as exc:
            LOGGER.warning("cn_tickflow_suspension_lookup_failed", error_type=type(exc).__name__)
    for symbol in missing.keys() & suspended.keys():
        pending = {day for day in missing[symbol] if day < suspended[symbol].first_day}
        if pending:
            missing[symbol] = pending
        else:
            del missing[symbol]
    rows = asyncio.run(_download(missing, target)) if missing else []
    inserted = 0
    if rows:
        stmt = insert(CACHE).on_conflict_do_nothing(index_elements=["symbol", "adjust", "date"]).returning(CACHE.c.symbol)
        with engine.begin() as connection:
            for offset in range(0, len(rows), DB_WRITE_BATCH_SIZE):
                inserted += len(connection.execute(stmt, rows[offset:offset + DB_WRITE_BATCH_SIZE]).all())
    remaining = _missing_days(engine, [symbol for symbol in symbols if symbol not in suspended], {target})
    LOGGER.info("cn_tickflow_prefill_done", target_date=str(target), active=len(symbols), symbols_requested=len(missing),
                rows_inserted=inserted, suspended=len(suspended.keys() & set(symbols)), target_date_missing=len(remaining))
    return inserted, len(remaining)


def main() -> int:
    """从定时任务冻结的名单和交易日历批量预填生产行情库。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--recent-days", type=int, required=True)
    args = parser.parse_args()
    if args.recent_days < 0:
        parser.error("--recent-days 不得为负数")
    if not (postgres_enabled() and postgres_configured()):
        parser.error("TickFlow 批量日线需要 PostgreSQL 配置")
    engine = _cache_engine()
    try:
        _, missing = prefill(load_plan(args.plan), engine, recent_days=args.recent_days)
    finally:
        engine.dispose()
    # 数据源大面积缺当日 K 线时，阻止整池退回到原本按只限频的路径。
    if missing > MAX_TARGET_FALLBACK_SYMBOLS:
        LOGGER.error("cn_tickflow_target_coverage_low", target_date_missing=missing, fallback_limit=MAX_TARGET_FALLBACK_SYMBOLS)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
