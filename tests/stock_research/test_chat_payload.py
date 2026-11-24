"""问答工具的真实证据体积分页及引用边界，不调用模型网络。"""

import asyncio
from pathlib import Path

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import BaseModel

from stock_research.chat_evidence import ChatAudit, ChatEvidence, validate_chat_answer


def test_normal_large_tool_output_is_bounded_and_pageable(tmp_path: Path) -> None:
    """【正常值】大结果分页可完整读回；未返回页不能提前充当已读证据。"""

    class Data(BaseModel):
        values: list[str]

    async def scenario() -> None:
        audit = ChatAudit("a" * 64, tmp_path / "evidence.json")

        async def query() -> Data:
            return Data(values=[f"fact-{i}:" + "x" * 200 for i in range(600)])

        first = await audit.respond("fixture", query)

        assert len(first.model_dump_json().encode()) <= 48 * 1024
        assert first.next_offset is not None
        registry = ChatEvidence.model_validate_json(audit.path.read_text())
        assert len(registry.facts) == len(first.facts) < first.total_facts == 601
        unread = first.facts[0].ref.rsplit("/", 2)[0] + "/values/599"
        assert unread not in registry.facts
        returned = list(first.facts)
        offset = first.next_offset
        while offset is not None:
            page = await audit.respond("fixture", query, fact_offset=offset)
            assert len(page.model_dump_json().encode()) <= 48 * 1024
            returned.extend(page.facts)
            offset = page.next_offset
        assert len({row.ref for row in returned}) == first.total_facts
        assert {row.value for row in returned if isinstance(row.value, str)} == set((await query()).values)

    asyncio.run(scenario())


def test_boundary_repeated_pages_obey_total_response_budget(tmp_path: Path) -> None:
    """【边界值】重复工具页也计入体积预算，拒绝后不登记额外成功。"""

    class Data(BaseModel):
        values: list[str]

    async def scenario() -> None:
        audit = ChatAudit("a" * 64, tmp_path / "budget.json")

        async def query() -> Data:
            return Data(values=["x" * 300 for _ in range(500)])

        for _ in range(5):
            await audit.respond("fixture", query)
        before = ChatEvidence.model_validate_json(audit.path.read_text())

        with pytest.raises(ToolError, match="足够证据"):
            await audit.respond("fixture", query)
        after = ChatEvidence.model_validate_json(audit.path.read_text())

        assert after.calls == 6 and after.successes == 5
        assert after.facts == before.facts

    asyncio.run(scenario())


def test_normal_stock_history_omits_unrequested_strategy_payload(tmp_path: Path) -> None:
    """【正常值】查历史不重复携带策略参数/意见；删掉的字段不登记为已读。"""

    class Data(BaseModel):
        company: dict[str, str]
        history: list[dict[str, object]]
        history_total: int
        observations: list[dict[str, object]]

    async def scenario() -> None:
        audit = ChatAudit("a" * 64, tmp_path / "history.json")

        async def query() -> Data:
            return Data(
                company={"symbol": "AAPL", "name": "Apple"},
                history=[{"ref_prefix": "bar/stock/AAPL/2026-11-20", "close": 11}],
                history_total=320,
                observations=[{"parameters": {"unused": "large" * 20000}}],
            )

        output = await audit.respond("stock_history", query)
        evidence = ChatEvidence.model_validate_json(audit.path.read_text())

        assert len(output.model_dump_json()) < 3000
        assert evidence.facts["bar/stock/AAPL/2026-11-20/close"].value == 11
        assert not any("observations" in ref for ref in evidence.facts)
        with pytest.raises(ValueError, match="引用"):
            validate_chat_answer(
                '{"context_hash":"' + "a" * 64 + '","answer":{"text":"未读取参数",'
                '"evidence_refs":["observations/parameters/unused"]},"observations":[],"risks":[], '
                '"confirmation_conditions":[],"claims":[{"ref":"bar/stock/AAPL/2026-11-20/close","value":11}]}',
                evidence,
                "a" * 64,
            )

    asyncio.run(scenario())
