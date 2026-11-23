"""模型观点的结构、范围与数值引用契约；不把文本判断当作已验证事实。"""

import math
import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from stock_research.evidence import EvidenceContext
from stock_research.identity import canonical_json
from stock_research.models import FrozenModel, normalize_symbol


class ResearchPoint(FrozenModel):
    """每个实质观点均引用事实；数值单独放入可逐值校验的claims。"""

    text: str = Field(min_length=1, max_length=800, description="纯文字观点，数值放claims；允许EMA等指标名称")
    evidence_refs: tuple[str, ...] = Field(min_length=1, max_length=8)

    @field_validator("text")
    @classmethod
    def reject_unstructured_numbers(cls, value: str) -> str:
        """避免模型把未引用价格或收益数字藏进自由文本。"""
        if re.search(r"(?<![A-Za-z0-9_])[-+]?\d+(?:\.\d+)?", value):
            raise ValueError("数值须放claims，不能混入观点文本")
        return value


class EvidenceClaim(FrozenModel):
    """只复述精确证据值，不接受由模型临时计算的新指标。"""

    ref: str
    value: str | float | bool | None


class SelectionItem(FrozenModel):
    """一只研究观察股票的理由、风险及待确认条件。"""

    symbol: str
    rationale: ResearchPoint
    risks: tuple[ResearchPoint, ...] = Field(min_length=1, max_length=4)
    confirmation_conditions: tuple[ResearchPoint, ...] = Field(min_length=1, max_length=4)
    claims: tuple[EvidenceClaim, ...] = Field(min_length=1, max_length=12)
    _normalize = field_validator("symbol")(normalize_symbol)


class SelectionDecision(FrozenModel):
    """正式初选只有通过引用验证并提交后才获得selection_id。"""

    role: Literal["select"] = "select"
    strategy: Literal["martin", "wyckoff"]
    context_hash: str
    items: tuple[SelectionItem, ...] = Field(max_length=5)
    empty_reason: ResearchPoint | None = None

    @model_validator(mode="after")
    def validate_empty(self) -> "SelectionDecision":
        """空结果必须明确说明，非空结果不接受矛盾的空选原因。"""
        if bool(self.items) == (self.empty_reason is not None):
            raise ValueError("空初选须有empty_reason，非空初选须为null")
        return self


class ReviewItem(FrozenModel):
    """针对不可变初选版本的一份独立策略意见。"""

    symbol: str
    stance: Literal["support", "concern", "insufficient_evidence"]
    rationale: ResearchPoint
    objections: tuple[ResearchPoint, ...] = Field(max_length=4)
    confirmation_conditions: tuple[ResearchPoint, ...] = Field(max_length=4)
    claims: tuple[EvidenceClaim, ...] = Field(min_length=1, max_length=12)
    data_limitations: tuple[str, ...] = Field(
        description="只能逐字选取固定输入limitations中的条目；没有适用条目时填空数组"
    )
    _normalize = field_validator("symbol")(normalize_symbol)


class ReviewDecision(FrozenModel):
    """每个目标股票必须有且只有一份意见，不改变初选原文。"""

    role: Literal["review"] = "review"
    strategy: Literal["martin", "wyckoff"]
    context_hash: str
    target_selection_id: str
    items: tuple[ReviewItem, ...] = Field(max_length=5)


def same_value(actual: object, expected: object) -> bool:
    """布尔不冒充零一；有限数字只容忍JSON往返的舍入误差。"""
    if isinstance(expected, bool) or isinstance(actual, bool):
        return type(actual) is type(expected) and actual == expected
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        return math.isclose(actual, expected, rel_tol=1e-6, abs_tol=1e-8)
    return type(actual) is type(expected) and actual == expected


def validate_item(context: EvidenceContext, item: SelectionItem | ReviewItem) -> None:
    """每条观点有有效引用，每股至少复述一个自身数值或布尔条件。"""
    points = (
        item.rationale,
        *item.confirmation_conditions,
        *(item.risks if isinstance(item, SelectionItem) else item.objections),
    )
    references = {ref for point in points for ref in point.evidence_refs}
    for ref in references:
        if context.resolve(ref).symbol not in {None, item.symbol}:
            raise ValueError("引用了另一只股票的事实")
    substantive = False
    for claim in item.claims:
        fact = context.resolve(claim.ref)
        if fact.symbol not in {None, item.symbol}:
            raise ValueError(f"claim引用了其他股票的事实：{claim.ref}")
        if not same_value(claim.value, fact.value):
            raise ValueError(f"声明数值与固定证据不一致：{claim.ref}，应为{canonical_json(fact.value)}")
        substantive |= fact.symbol == item.symbol and isinstance(fact.value, (float, bool))
    if not substantive:
        raise ValueError("缺少该股自身的数值或条件证据")


def validate_decision(
    context: EvidenceContext, text: str, *, target_selection_id: str | None = None
) -> SelectionDecision | ReviewDecision:
    """范围和数值可机械校验，纯文字立场仍只是模型观点。"""
    model = SelectionDecision if context.role == "select" else ReviewDecision
    decision = model.model_validate_json(text)
    if decision.strategy != context.strategy or decision.context_hash != context.content_hash:
        raise ValueError("模型输出引用了其他策略或阶段")
    symbols = [item.symbol for item in decision.items]
    if len(set(symbols)) != len(symbols) or not set(symbols) <= set(context.symbols):
        raise ValueError("模型返回池外、目标外或重复证券")
    if isinstance(decision, ReviewDecision):
        if decision.target_selection_id != target_selection_id or set(symbols) != set(context.symbols):
            raise ValueError("互评引用了错误初选版本或遗漏目标")
        for item in decision.items:
            if not set(item.data_limitations) <= set(context.limitations):
                raise ValueError("data_limitations只能逐字选取固定limitations，或填空数组；不能补写摘要")
            available = context.facts.get(f"{context.strategy}/{item.symbol}/calculation_available")
            if available is not None and available.value is False and item.stance != "insufficient_evidence":
                raise ValueError(f"{item.symbol}缺少本策略计算，stance须为insufficient_evidence")
    elif decision.empty_reason is not None:
        for ref in decision.empty_reason.evidence_refs:
            context.resolve(ref)
    for item in decision.items:
        validate_item(context, item)
    return decision
