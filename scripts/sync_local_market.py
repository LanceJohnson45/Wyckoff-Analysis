"""本机运行：从Actions维护的PostgreSQL增量同步日线。"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

from dotenv import load_dotenv

SYNC_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(SYNC_SCRIPT_ROOT))
load_dotenv(SYNC_SCRIPT_ROOT / ".env")

from integrations.local_market_store import local_market_summary
from integrations.market_data_sync import sync_market, sync_symbols


def main() -> int:
    """首次复制已有历史，以后按更新时间同步；失败返回非零。"""
    parser = argparse.ArgumentParser(description="从PostgreSQL同步到本机行情库，不调用在线行情源")
    parser.add_argument("--market", choices=["cn", "us", "all"], default="all")
    parser.add_argument("--symbols", default="", help="可选自选列表；留空同步本市场全部云端股票。自动包含市场基准。")
    parser.add_argument("--full", action="store_true", help="忽略水位，重读云端现有历史，不删除本地旧历史")
    args = parser.parse_args()
    if args.market == "all" and args.symbols:
        parser.error("指定 --symbols 时必须选择 cn 或 us")
    markets = ("cn", "us") if args.market == "all" else (args.market,)
    has_issues = False
    for market in markets:
        try:
            result = sync_market(market, symbols=sync_symbols(args.symbols, market), full=args.full,
                                 progress=lambda rows: sys.stderr.write(f"已写入/核对 {rows} 行\n"))
            summary = local_market_summary(market)
            sys.stdout.write(json.dumps({"sync": asdict(result), "local": summary}, ensure_ascii=False) + "\n")
            has_issues = has_issues or result.rejected > 0 or int(summary["issues"]) > 0
        except (ValueError, RuntimeError, OSError) as exc:
            sys.stderr.write(f"同步失败：{exc}\n")
            return 1
    return 2 if has_issues else 0


if __name__ == "__main__":
    raise SystemExit(main())
