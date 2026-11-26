"""Personal等外部交互入口的三项只读股票工具，不持有PG或模型身份。"""

from collections.abc import Callable
from datetime import date
from typing import Annotated

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import UUID4, Field, ValidationError

from stock_research.query_client import QueryError, ResearchReader
from stock_research.query_pages import QueryPages
from stock_research.query_settings import QuerySettings
from stock_research.schemas.query_pages import QueryPage


def create_server(reader: ResearchReader) -> FastMCP:
    """注册范围固定，不提供运行动作、shell、资源目录或通用HTTP。"""
    server = FastMCP("stock-research-query")
    pages = QueryPages()
    annotations = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)

    @server.tool(annotations=annotations)
    async def stock_research_latest_report(
        target_session: date | None = None, cursor: Annotated[str | None, Field(max_length=64)] = None
    ) -> QueryPage:
        """读最新报告JSON分片；next_cursor非空须保持原参数续读，同result_id才可合并。scope保留日期/版本。"""
        try:
            return await pages.respond(
                "stock_research_latest_report",
                {"target_session": str(target_session) if target_session else None},
                lambda: reader.latest_report(target_session),
                cursor,
            )
        except QueryError as error:
            raise ToolError(str(error)) from None

    @server.tool(annotations=annotations)
    async def stock_research_stock(
        symbol: Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")],
        run_id: UUID4,
        limit: Annotated[int, Field(ge=1, le=320)] = 60,
        report_id: UUID4 | None = None,
        cursor: Annotated[str | None, Field(max_length=64)] = None,
    ) -> QueryPage:
        """按run查股票完整JSON分片；追问传report_id固定观点。next_cursor非空保持原参数续读，未读页不等于无事实。"""
        try:
            return await pages.respond(
                "stock_research_stock",
                {
                    "symbol": symbol,
                    "run_id": str(run_id),
                    "limit": limit,
                    "report_id": str(report_id) if report_id else None,
                },
                lambda: reader.stock(symbol, run_id, limit, report_id),
                cursor,
            )
        except QueryError as error:
            raise ToolError(str(error)) from None

    @server.tool(annotations=annotations)
    async def stock_research_run_status(
        run_id: UUID4, cursor: Annotated[str | None, Field(max_length=64)] = None
    ) -> QueryPage:
        """读运行JSON分片；next_cursor非空保持run_id续读同一观察时点，省略cursor才刷新；completed不等于通知sent。"""
        try:
            return await pages.respond(
                "stock_research_run_status", {"run_id": str(run_id)}, lambda: reader.run_status(run_id), cursor
            )
        except QueryError as error:
            raise ToolError(str(error)) from None

    return server


def main(factory: Callable[[ResearchReader], FastMCP] = create_server) -> None:
    """启动仅加载专用查询配置，stdio stdout保留给协议。"""
    try:
        settings = QuerySettings()
    except (ValidationError, OSError):
        raise SystemExit("只读研究配置无效，请检查专用授权文件及服务端口；不输出配置原值") from None
    factory(ResearchReader(settings)).run(transport="stdio")


if __name__ == "__main__":
    main()
