"""可被其他Agent调用的只读HTTP命令，不加载研究数据库设置。"""

import argparse
import asyncio
import sys
from datetime import date

from pydantic import UUID4, TypeAdapter, ValidationError

from stock_research.query_client import QueryError, ResearchReader
from stock_research.query_settings import QuerySettings


def main(argv: list[str] | None = None) -> int:
    """JSON输出与退出码；身份来自专用配置，不能由命令参数泄漏。"""
    parser = argparse.ArgumentParser(prog="python -m stock_research.query_cli")
    commands = parser.add_subparsers(dest="command", required=True)
    latest = commands.add_parser("latest-report")
    latest.add_argument("--target-session", type=date.fromisoformat)
    status = commands.add_parser("run-status")
    status.add_argument("--run-id", required=True)
    stock = commands.add_parser("stock")
    stock.add_argument("--symbol", required=True)
    stock.add_argument("--run-id", required=True)
    stock.add_argument("--report-id")
    stock.add_argument("--limit", type=int, default=60)
    anchor = commands.add_parser("anchor-chart")
    anchor.add_argument("--symbol", required=True)
    anchor.add_argument("--report-id", required=True)
    anchor.add_argument("--limit", type=int, default=320)
    anchor.add_argument("--index", type=int)
    attempts = commands.add_parser("quant-attempts")
    attempts.add_argument("--run-id", required=True)
    attempts.add_argument("--page", type=int, default=1)
    attempts.add_argument("--page-size", type=int, default=10)
    args = parser.parse_args(argv)
    try:
        reader = ResearchReader(QuerySettings())
        if args.command == "latest-report":
            operation = reader.latest_report(args.target_session)
        elif args.command == "anchor-chart":
            operation = reader.anchor_chart(
                TypeAdapter(UUID4).validate_python(args.report_id), args.symbol, args.limit, args.index
            )
        else:
            run_id = TypeAdapter(UUID4).validate_python(args.run_id)
            if args.command == "quant-attempts":
                operation = reader.quant_attempts(run_id, args.page, args.page_size)
            elif args.command == "run-status":
                operation = reader.run_status(run_id)
            else:
                report_id = TypeAdapter(UUID4).validate_python(args.report_id) if args.report_id else None
                operation = reader.stock(args.symbol, run_id, args.limit, report_id)
        print(asyncio.run(operation).model_dump_json(indent=2))
        return 0
    except ValidationError:
        print('{"error":"只读身份或查询参数无效，请检查专用查询配置"}', file=sys.stderr)
    except QueryError as error:
        import json

        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
