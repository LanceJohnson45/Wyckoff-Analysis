"""固定证据的范围、截止日和模型输出契约。"""

from datetime import timedelta

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from stock_research.decisions import validate_decision
from stock_research.evidence import EvidenceContext
from stock_research.evidence_mcp import EvidenceReader
from stock_research.identity import digest, new_id, now


def make_context(**changes: object) -> EvidenceContext:
    """显式合成契约样本，不用它替代生产策略信号。"""
    body = dict(
        workspace="test",
        run_id=new_id(),
        stage_id=new_id(),
        strategy="martin",
        role="select",
        snapshot_id=new_id(),
        snapshot_hash="a" * 64,
        analysis_hash="b" * 64,
        as_of="2026-11-20",
        deadline=(now() + timedelta(seconds=120)).isoformat(),
        tool_budget=20,
        pool=["AAPL"],
        symbols=["AAPL"],
        limitations=["synthetic fixture"],
        facts={
            "scope/pool_size": {"ref": "scope/pool_size", "symbol": None, "value": 1.0},
            "martin/AAPL/factors/close": {"ref": "martin/AAPL/factors/close", "symbol": "AAPL", "value": 11.0},
            "martin/AAPL/qualified": {"ref": "martin/AAPL/qualified", "symbol": "AAPL", "value": True},
        },
        histories={"stock:AAPL": [{"date": "2026-11-20", "close": 11.0}]},
    )
    body.update(changes)
    return EvidenceContext(**body, content_hash=digest(body))


def decision_body(context: EvidenceContext) -> dict[str, object]:
    """所有观点引用同一项可核验事实，便于定向破坏各个契约。"""
    ref = "martin/AAPL/factors/close"
    point = {"text": "等待支撑确认", "evidence_refs": [ref]}
    return {
        "strategy": context.strategy,
        "context_hash": context.content_hash,
        "items": [
            {
                "symbol": "AAPL",
                "rationale": point,
                "risks": [point],
                "confirmation_conditions": [point],
                "claims": [{"ref": ref, "value": 11.0}],
            }
        ],
    }


def test_reader_denies_foreign_symbol_and_counts_invalid_calls() -> None:
    """无效查询也消耗本阶段预算，不能通过枚举扩大授权。"""
    reader = EvidenceReader(make_context(tool_budget=2))
    assert reader.evidence("catalog")["symbols"] == ["AAPL"]
    with pytest.raises(ToolError, match="授权"):
        reader.evidence("stock/MSFT")
    with pytest.raises(ToolError, match="预算"):
        reader.evidence("stock/AAPL")


def test_reader_history_is_fixed_and_bounded() -> None:
    reader = EvidenceReader(make_context())
    assert reader.history("AAPL", "stock", 1)["rows"][0]["ref_prefix"] == "bar/stock/AAPL/2026-11-20"
    assert reader.context.resolve("bar/stock/AAPL/2026-11-20/close").value == 11.0
    for symbol, limit in (("MSFT", 1), ("AAPL", 61), ("AAPL", True)):
        with pytest.raises(ToolError):
            reader.history(symbol, "stock", limit)
    with pytest.raises(ValueError):
        reader.context.resolve("bar/stock/AAPL/2026-11-21/close")


def test_context_rejects_tamper_and_future_history() -> None:
    context = make_context()
    body = context.model_dump(mode="json")
    body["workspace"] = "other"
    with pytest.raises(ValueError, match="摘要"):
        EvidenceContext.model_validate(body)
    with pytest.raises(ValueError, match="截止日"):
        make_context(histories={"stock:AAPL": [{"date": "2026-11-23", "close": 12}]})
    with pytest.raises(ToolError, match="期限"):
        EvidenceReader(make_context(deadline=(now() - timedelta(seconds=1)).isoformat())).evidence("catalog")


def test_selection_accepts_only_scoped_numeric_claims() -> None:
    import json

    context = make_context()
    body = decision_body(context)
    decision = validate_decision(context, json.dumps(body))
    assert decision.items[0].symbol == "AAPL"
    body["items"][0]["claims"][0]["value"] = 12
    with pytest.raises(ValueError, match="数值"):
        validate_decision(context, json.dumps(body))
    body = decision_body(context)
    body["items"] *= 2
    with pytest.raises(ValueError, match="重复"):
        validate_decision(context, json.dumps(body))
    body = decision_body(context)
    body["items"][0]["symbol"] = "MSFT"
    with pytest.raises(ValueError, match="池外"):
        validate_decision(context, json.dumps(body))


@pytest.mark.parametrize("value", [1, 1.0, "true", None])
def test_boolean_fact_is_not_a_numeric_or_string_claim(value: object) -> None:
    import json

    context = make_context()
    body = decision_body(context)
    body["items"][0]["rationale"]["evidence_refs"].append("martin/AAPL/qualified")
    body["items"][0]["claims"] = [{"ref": "martin/AAPL/qualified", "value": value}]
    with pytest.raises(ValueError, match="数值"):
        validate_decision(context, json.dumps(body))


def test_review_requires_every_target_and_immutable_selection_id() -> None:
    import json

    context = make_context(role="review", pool=[])
    item = decision_body(context)["items"][0]
    item.pop("risks")
    item.update(stance="concern", objections=[], data_limitations=[])
    body = {
        "role": "review",
        "strategy": "martin",
        "context_hash": context.content_hash,
        "target_selection_id": "committed-id",
        "items": [item],
    }
    assert validate_decision(context, json.dumps(body), target_selection_id="committed-id").items[0].stance == "concern"
    with pytest.raises(ValueError, match="版本"):
        validate_decision(context, json.dumps(body), target_selection_id="other-id")
    body["items"] = []
    with pytest.raises(ValueError, match="遗漏"):
        validate_decision(context, json.dumps(body), target_selection_id="committed-id")


def test_verified_claim_can_cite_its_own_ref_without_duplicate_prose_reference() -> None:
    """结构化claim自身已有引用，正确数值不需要在纯文字段重复列举。"""
    import json

    context = make_context()
    body = decision_body(context)
    body["items"][0]["rationale"]["evidence_refs"] = ["martin/AAPL/qualified"]
    assert validate_decision(context, json.dumps(body)).items[0].claims[0].value == 11
    body["items"][0]["claims"][0]["value"] = 12
    with pytest.raises(ValueError, match="martin/AAPL/factors/close，应为11.0"):
        validate_decision(context, json.dumps(body))


def test_review_prompt_schema_enumerates_only_registered_limitations() -> None:
    import json

    from stock_research.prompts import stage_prompt

    context = make_context(role="review", pool=[])
    packet = json.loads(stage_prompt(context, {"selection_id": "fixture"}).split("固定阶段输入：\n", 1)[1])
    assert packet["schema"]["$defs"]["ReviewItem"]["properties"]["data_limitations"]["items"]["enum"] == list(
        context.limitations
    )
