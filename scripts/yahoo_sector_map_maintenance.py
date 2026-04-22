from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime


if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integrations.sector_map_yfinance import cache_path, refresh_sector_cache


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build unified Yahoo sector/industry cache for CN/HK/US funnels"
    )
    parser.add_argument(
        "--markets",
        nargs="+",
        default=["cn", "hk", "us"],
        choices=["cn", "hk", "us"],
    )
    parser.add_argument("--delay-seconds", type=float, default=1.2)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--max-symbols-per-market", type=int, default=0)
    parser.add_argument("--checkpoint-every", type=int, default=20)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    _log(
        f"sector cache refresh start markets={args.markets} delay={args.delay_seconds}s retries={args.retries} "
        f"max_symbols_per_market={args.max_symbols_per_market} checkpoint_every={args.checkpoint_every} force={bool(args.force)}"
    )
    stats = refresh_sector_cache(
        markets=list(args.markets),
        delay_seconds=max(float(args.delay_seconds), 0.0),
        retries=max(int(args.retries), 0),
        max_symbols_per_market=max(int(args.max_symbols_per_market), 0),
        force=bool(args.force),
        checkpoint_every=max(int(args.checkpoint_every), 1),
    )
    _log(f"sector cache refresh done path={cache_path()}")
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
