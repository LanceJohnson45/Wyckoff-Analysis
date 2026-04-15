# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.stock_cache import get_cache_meta
from integrations.fetch_a_share_csv import _resolve_trading_window, _resolve_us_window
from integrations.stock_hist_repository import get_stock_hist
from scripts.wyckoff_funnel import _job_end_calendar_day, _normalize_symbols, _resolve_funnel_market, _resolve_symbol_pool_from_env


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _prefetch_one(symbol: str, market: str, trading_days: int) -> tuple[str, int]:
    end_day = _job_end_calendar_day()
    window = (
        _resolve_us_window(end_calendar_day=end_day, trading_days=trading_days)
        if market in {"us", "hk"}
        else _resolve_trading_window(end_calendar_day=end_day, trading_days=trading_days)
    )
    frame = get_stock_hist(
        symbol=symbol,
        start_date=window.start_trade_date,
        end_date=window.end_trade_date,
        adjust="qfq",
        market=market,
        context="background",
    )
    if market == "us":
        meta = get_cache_meta(f"US:{symbol}", "qfq", context="background")
    elif market == "hk":
        meta = get_cache_meta(f"HK:{symbol}", "qfq", context="background")
    else:
        meta = get_cache_meta(symbol, "qfq", context="background")
    if meta is None or meta.end_date < window.end_trade_date:
        raise RuntimeError(f"cache verification failed end_date={getattr(meta, 'end_date', None)} target={window.end_trade_date}")
    return symbol, int(len(frame.index))


def main() -> int:
    parser = argparse.ArgumentParser(description="Warm funnel cache for the current symbol pool")
    parser.add_argument("--market", choices=["cn", "us", "hk"], default=None)
    parser.add_argument("--trading-days", type=int, default=max(int(os.getenv("FUNNEL_TRADING_DAYS", "320")), 30))
    parser.add_argument("--max-workers", type=int, default=max(int(os.getenv("FUNNEL_PREWARM_MAX_WORKERS", "8")), 1))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    args = parser.parse_args()

    if args.market:
        os.environ["FUNNEL_MARKET"] = args.market
    market = _resolve_funnel_market()
    symbols, _, stats = _resolve_symbol_pool_from_env()
    normalized = _normalize_symbols(symbols, market=market)
    if args.limit > 0:
        normalized = normalized[: args.limit]
    if not normalized:
        _log(f"prewarm skipped market={market} reason=empty_symbol_pool mode={stats.get('pool_mode')}")
        return 0

    _log(
        f"prewarm start market={market} symbols={len(normalized)} trading_days={args.trading_days} mode={stats.get('pool_mode')}"
    )
    ok = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=max(int(args.max_workers), 1)) as executor:
        futures = {executor.submit(_prefetch_one, sym, market, max(int(args.trading_days), 1)): sym for sym in normalized}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                _, rows = future.result()
                ok += 1
                _log(f"prewarm ok {symbol} rows={rows}")
            except Exception as e:
                fail += 1
                _log(f"prewarm fail {symbol}: {type(e).__name__}: {e}")
            if args.sleep_seconds > 0:
                time.sleep(max(float(args.sleep_seconds), 0.0))
    _log(f"prewarm done market={market} ok={ok} fail={fail}")
    return 0 if ok > 0 or fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
