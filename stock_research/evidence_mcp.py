"""独立stdio工具服务；固定事实查询不提供文件、SQL或联网入口。"""

import os
from datetime import datetime
from pathlib import Path
from typing import Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from stock_research.evidence import EvidenceContext


class EvidenceReader:
    """一个进程对应一个阶段，查询预算跨结构修正轮次累计。"""

    def __init__(self, context: EvidenceContext) -> None:
        self.context = context
        self.calls = 0

    def admit(self) -> None:
        """无效查询也消耗预算，避免越权枚举和无限重试。"""
        self.calls += 1
        if self.calls > self.context.tool_budget or datetime.now(self.context.deadline.tzinfo) >= self.context.deadline:
            raise ToolError("阶段工具预算或期限已耗尽")

    def evidence(self, ref: str) -> dict[str, object]:
        """返回精确引用；catalog列举本阶段可用的全部标量引用。"""
        self.admit()
        if ref == "catalog":
            return {
                "context_hash": self.context.content_hash,
                "symbols": list(self.context.symbols),
                "global_refs": [key for key, item in self.context.facts.items() if item.symbol is None][:128],
                "usage": "stock/SYMBOL列出该股事实；精确ref查询单项；历史通过history查询",
            }
        if ref.startswith("stock/"):
            symbol = ref.removeprefix("stock/")
            if symbol not in self.context.symbols:
                raise ToolError("证券不在当前阶段授权中")
            rows = [item.model_dump(mode="json") for item in self.context.facts.values() if item.symbol == symbol]
            return {"symbol": symbol, "facts": rows[:512], "truncated": len(rows) > 512}
        try:
            return self.context.resolve(ref).model_dump(mode="json")
        except ValueError:
            raise ToolError("引用不在当前阶段授权中") from None

    def history(self, symbol: str, kind: Literal["stock", "benchmark"], limit: int) -> dict[str, object]:
        """只读固定截止日的最近记录，不在缺失时联网补数据。"""
        self.admit()
        if isinstance(limit, bool) or not 1 <= limit <= 60 or f"{kind}:{symbol}" not in self.context.histories:
            raise ToolError("历史查询范围无效")
        rows = self.context.histories[f"{kind}:{symbol}"][-limit:]
        return {
            "symbol": symbol,
            "kind": kind,
            "as_of": str(self.context.as_of),
            "rows": [{**row, "ref_prefix": f"bar/{kind}/{symbol}/{row['date']}"} for row in rows],
        }


def main() -> None:
    """仅operator提供固定文件；model工具没有路径参数。"""
    context = EvidenceContext.model_validate_json(Path(os.environ["STOCK_RESEARCH_CONTEXT_FILE"]).read_text())
    reader = EvidenceReader(context)
    server = FastMCP("stock-research-evidence")

    @server.tool()
    def evidence(ref: str) -> dict[str, object]:
        """查询固定证据引用，ref=catalog列出本阶段全部已注册事实。"""
        return reader.evidence(ref)

    @server.tool()
    def history(symbol: str, kind: Literal["stock", "benchmark"] = "stock", limit: int = 20) -> dict[str, object]:
        """查询已授权证券的截止日日线，最多60根；引用为ref_prefix/字段名。"""
        return reader.history(symbol, kind, limit)

    server.run(transport="stdio")


if __name__ == "__main__":
    main()
