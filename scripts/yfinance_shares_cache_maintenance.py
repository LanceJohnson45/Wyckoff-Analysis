from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations.fetch_a_share_csv import get_stocks_by_board
from integrations.hk_index_universe import get_hk_index_union
from integrations.us_sp500_universe import get_sp500_constituents
from integrations.yfinance_enrichment import (
    _SHARES_CACHE_TTL,
    _SHARES_CACHE_PATH,
    _fetch_share_record,
    _is_fresh,
    _load_shares_items,
    _save_shares_items,
    normalize_yfinance_symbol,
)


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def _targets_for_market(market: str) -> list[str]:
    market_norm = str(market or "cn").strip().lower()
    if market_norm == "us":
        return list(get_sp500_constituents(prefer_snapshot=True).symbols)
    if market_norm == "hk":
        return list(get_hk_index_union(prefer_snapshot=True).symbols)
    items = get_stocks_by_board("main") + get_stocks_by_board("chinext")
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        code = str(item.get("code", "")).strip()
        yf_symbol = normalize_yfinance_symbol(code, "cn")
        if yf_symbol and yf_symbol not in seen:
            seen.add(yf_symbol)
            out.append(yf_symbol)
    return out


def _targets_from_args(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.symbol:
        out: list[tuple[str, str]] = []
        for raw in args.symbol:
            if ":" in raw:
                market, symbol = raw.split(":", 1)
            else:
                market, symbol = args.markets[0], raw
            market_norm = str(market or "").strip().lower()
            if market_norm not in {"cn", "hk", "us"}:
                raise ValueError(f"unsupported market in --symbol: {raw}")
            yf_symbol = normalize_yfinance_symbol(symbol, market_norm)
            if yf_symbol:
                out.append((market_norm, yf_symbol))
        return out

    targets: list[tuple[str, str]] = []
    for market in args.markets:
        for yf_symbol in _targets_for_market(market):
            targets.append((market, yf_symbol))
    return targets


def _dedupe_targets(targets: list[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for market, symbol in targets:
        key = f"{market}:{symbol}"
        if key in seen:
            continue
        seen.add(key)
        out.append((market, symbol))
    return out


def refresh_shares_cache(
    targets: list[tuple[str, str]],
    *,
    delay_seconds: float,
    force: bool,
    checkpoint_every: int,
    retries: int,
) -> dict[str, Any]:
    import yfinance as yf

    items = _load_shares_items()
    stats: dict[str, Any] = {
        "total_targets": len(targets),
        "fetched": 0,
        "cache_hits": 0,
        "failed": 0,
        "skipped": 0,
        "by_market": {},
        "cache_path": str(_SHARES_CACHE_PATH),
    }
    for market, _ in targets:
        stats["by_market"].setdefault(market, {"targets": 0, "fetched": 0, "cache_hits": 0, "failed": 0})
        stats["by_market"][market]["targets"] += 1

    changed = False
    for idx, (market, yf_symbol) in enumerate(targets, start=1):
        row = items.get(yf_symbol)
        if not force and isinstance(row, dict) and _is_fresh(row, _SHARES_CACHE_TTL):
            stats["cache_hits"] += 1
            stats["by_market"][market]["cache_hits"] += 1
            if idx % max(checkpoint_every, 1) == 0:
                _log(f"progress {idx}/{len(targets)} cache_hits={stats['cache_hits']} fetched={stats['fetched']} failed={stats['failed']}")
            continue

        ok = False
        last_error = ""
        for attempt in range(max(retries, 0) + 1):
            try:
                record = _fetch_share_record(yf, yf_symbol, market)
                if not record.get("shares_outstanding"):
                    raise RuntimeError("shares_outstanding_missing")
                items[yf_symbol] = record
                stats["fetched"] += 1
                stats["by_market"][market]["fetched"] += 1
                changed = True
                ok = True
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    time.sleep(max(delay_seconds, 0.0))

        if not ok:
            items[yf_symbol] = {
                "symbol": yf_symbol,
                "market": market,
                "error": last_error,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "updated_ts": time.time(),
            }
            stats["failed"] += 1
            stats["by_market"][market]["failed"] += 1
            changed = True
            _log(f"failed {market}:{yf_symbol} {last_error}")

        if idx % max(checkpoint_every, 1) == 0:
            if changed:
                _save_shares_items(items)
                changed = False
            _log(f"progress {idx}/{len(targets)} cache_hits={stats['cache_hits']} fetched={stats['fetched']} failed={stats['failed']}")

        if delay_seconds > 0:
            time.sleep(delay_seconds)

    if changed:
        _save_shares_items(items)
    stats["cache_items"] = len(items)
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prebuild data/yfinance_shares_cache.json for market-cap calculation"
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["cn"],
        choices=["cn", "hk", "us"],
        help="Markets to refresh when --symbol is not provided",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="Custom symbol, optionally market-prefixed, e.g. cn:000001, us:MSFT, hk:9988",
    )
    parser.add_argument("--limit", type=int, default=0, help="Limit total targets for trial runs")
    parser.add_argument("--delay-seconds", type=float, default=0.6)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--checkpoint-every", type=int, default=20)
    parser.add_argument("--force", action="store_true", help="Refresh even fresh cache rows")
    args = parser.parse_args()

    targets = _dedupe_targets(_targets_from_args(args))
    if args.limit and args.limit > 0:
        targets = targets[: int(args.limit)]
    _log(
        f"shares cache refresh start targets={len(targets)} markets={args.markets} "
        f"delay={args.delay_seconds}s retries={args.retries} force={bool(args.force)}"
    )
    stats = refresh_shares_cache(
        targets,
        delay_seconds=max(float(args.delay_seconds), 0.0),
        force=bool(args.force),
        checkpoint_every=max(int(args.checkpoint_every), 1),
        retries=max(int(args.retries), 0),
    )
    _log(f"shares cache refresh done path={_SHARES_CACHE_PATH}")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0 if stats.get("fetched", 0) or stats.get("cache_hits", 0) else 1


if __name__ == "__main__":
    raise SystemExit(main())
