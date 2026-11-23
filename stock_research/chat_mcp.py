"""对话工具固定报告/运行，不允许模型改变数据源或获得写入能力。"""

import json
import os
from pathlib import Path
from typing import Annotated, Literal

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from stock_research.chat_evidence import ChatAudit, ChatToolOutput
from stock_research.query_client import ResearchReader
from stock_research.query_settings import QuerySettings
from stock_research.schemas.chat import ChatContext
from stock_research.schemas.report_queries import Symbol

CHAT_TOOLS = tuple(
    "mcp__chat__" + name
    for name in (
        "market_overview",
        "group_strength",
        "stock_factors",
        "stock_history",
        "compare_stocks",
        "strategy_rules",
        "report_read",
        "run_status",
    )
)


def create_server(reader: ResearchReader, context: ChatContext, audit: ChatAudit) -> FastMCP:
    """工具入参没有报告、URL、文件或配置；目录等于计划约定的八项能力。"""
    server = FastMCP("stock-research-chat")
    annotations = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

    @server.tool(annotations=annotations)
    async def market_overview(limit: Annotated[int, Field(ge=1, le=320)] = 5) -> ChatToolOutput:
        """固定报告的市场与三基准；pool/count仅为量化池数量，不是正式初选。"""
        return await audit.respond("market_overview", lambda: reader.market_overview(context.report_id, limit))

    @server.tool(annotations=annotations)
    async def group_strength(
        page: Annotated[int, Field(ge=1)] = 1,
        page_size: Annotated[int, Field(ge=1, le=20)] = 5,
        kind: Literal["industry", "theme"] | None = None,
        name: str | None = None,
    ) -> ChatToolOutput:
        """固定原分组强度SQL分页，保留覆盖、成员和缺失；name包含industry/theme前缀。"""
        return await audit.respond(
            "group_strength", lambda: reader.group_strength(context.report_id, page, page_size, kind, name)
        )

    @server.tool(annotations=annotations)
    async def stock_factors(symbol: Symbol) -> ChatToolOutput:
        """整池代码的两策略事实、排除、分组和固定修订意见；仅带最后一根行情。"""
        return await audit.respond("stock_factors", lambda: reader.stock(symbol, context.run_id, 1, context.report_id))

    @server.tool(annotations=annotations)
    async def stock_history(symbol: Symbol, limit: Annotated[int, Field(ge=1, le=320)] = 60) -> ChatToolOutput:
        """固定截止日的日线和证据路径，禁止当前行情补入历史。"""
        return await audit.respond(
            "stock_history", lambda: reader.stock(symbol, context.run_id, limit, context.report_id)
        )

    @server.tool(annotations=annotations)
    async def compare_stocks(
        symbols: Annotated[list[Symbol], Field(min_length=2, max_length=8)],
        limit: Annotated[int, Field(ge=1, le=60)] = 5,
    ) -> ChatToolOutput:
        """同快照比较，不合并策略评分或生成交易许可。"""
        return await audit.respond(
            "compare_stocks", lambda: reader.compare_stocks(context.report_id, tuple(symbols), limit)
        )

    @server.tool(annotations=annotations)
    async def strategy_rules() -> ChatToolOutput:
        """冻结参数、因子定义与有效配置，工程默认值不构成收益验证。"""
        return await audit.respond("strategy_rules", lambda: reader.strategy_rules(context.report_id))

    @server.tool(annotations=annotations)
    async def report_read() -> ChatToolOutput:
        """固定报告原初选与互评；items/count是该数组真实条目数，意见不是执行指令。"""
        return await audit.respond("report_read", lambda: reader.report(context.report_id))

    @server.tool(annotations=annotations)
    async def run_status() -> ChatToolOutput:
        """当前对应运行状态，observed_at反映查询时点，不改写固定报告。"""
        return await audit.respond("run_status", lambda: reader.run_status(context.run_id))

    return server


def main() -> None:
    """固定上下文和专用GET身份由受信进程注入，不加载根环境配置。"""
    try:
        path = Path(os.environ["STOCK_RESEARCH_CHAT_CONTEXT_FILE"])
        context = ChatContext.model_validate_json(path.read_text())
        identity = json.loads(Path(os.environ["STOCK_RESEARCH_CHAT_QUERY_FILE"]).read_text())
        settings = QuerySettings(_env_file=None, port=context.api_port, **identity)
        audit = ChatAudit(context.content_hash, path.parent / "chat-evidence.json")
        audit.persist()
    except (KeyError, ValueError, OSError):
        raise SystemExit("固定研究问答工具配置无效，不输出配置原值") from None
    create_server(ResearchReader(settings), context, audit).run(transport="stdio")


if __name__ == "__main__":
    main()
