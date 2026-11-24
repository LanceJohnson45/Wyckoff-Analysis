"""固定输入的历史量化池评估，正式模型初选仍单独验收。"""

import json
import time
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import UUID4, Field, model_validator

from stock_research.artifacts import write_once
from stock_research.calendar import sessions_ending
from stock_research.evaluation_outcomes import LONGEST_HORIZON, OutcomeWindow, indexed_outcomes, price_index
from stock_research.evaluation_sampling import SECTION_COUNT, EvaluationSection, sample_sections
from stock_research.identity import digest
from stock_research.models import Bar, ClassificationVersion, FrozenModel, QualityIssue, UniverseVersion
from stock_research.parameters import MartinParameters
from stock_research.quality import BENCHMARKS, HISTORY_REQUIREMENTS, build_snapshot
from stock_research.quant_compute import QuantParameters, compute_analysis, verify_analysis
from stock_research.source_guard import LOADED_SOURCE_IDENTITY, ensure_source_current
from stock_research.wyckoff import WyckoffParameters

EVALUATION_HISTORY = 400
EVALUATION_WARMUP = max(HISTORY_REQUIREMENTS.values())
BACKFILL_LIMITATION = "当前成分及当前分类回溯：派生有效期仅供量化描述，存在存活偏差及事后资料，不是严格时点回放"


class EvaluationInput(FrozenModel):
    """原输入全文只存一次，截面按日期切片后调用生产实现。"""

    cutoff: date
    source_identity: str
    universe: UniverseVersion
    classifications: ClassificationVersion
    bars: tuple[Bar, ...]
    issues: tuple[QualityIssue, ...] = ()

    @model_validator(mode="after")
    def verify_dates(self) -> "EvaluationInput":
        """固定截止日的输入不接收未来价格。"""
        if any(bar.session_date > self.cutoff for bar in self.bars):
            raise ValueError("评估输入含截止日之后行情")
        return self


class SectionAnalysis(FrozenModel):
    """完整量化证据另存小文件，池归属及分母始终可见。"""

    section: EvaluationSection
    strategy: Literal["martin", "wyckoff"]
    snapshot_hash: str
    parameter_hash: str
    analysis_hash: str
    analysis_file: str
    universe_count: int = Field(ge=0)
    eligible_count: int = Field(ge=0)
    observation_count: int = Field(ge=0)
    exclusions: dict[str, int]
    pool: tuple[str, ...]
    outcomes: dict[str, tuple[OutcomeWindow, ...]] | None


class SectionBinding(FrozenModel):
    """中断续跑引用的小清单也必须经过结构验证。"""

    id: UUID4
    content_hash: str = Field(pattern=r"^[a-f0-9]{64}$")


class EvaluationReport(FrozenModel):
    """此结果的量化池表现不能冒充模型正式入选表现。"""

    schema_version: Literal[1] = 1
    kind: Literal["historical_quant_pool"] = "historical_quant_pool"
    input_hash: str
    source_hash: str
    parameter_hashes: dict[str, str]
    cutoff: date
    allow_current_universe: bool
    candidate_count: int
    sections: tuple[SectionAnalysis, ...]
    usable_sections: dict[str, int]
    model_selection: None = None
    manual_review: Literal["pending"] = "pending"
    limitations: tuple[str, ...]
    content_hash: str

    @model_validator(mode="after")
    def verify_hash(self) -> "EvaluationReport":
        """损坏缓存不能被当作已经完成的实验。"""
        if digest(self.model_dump(mode="json", exclude={"content_hash"})) != self.content_hash:
            raise ValueError("评估产物摘要不一致")
        if len(self.sections) != SECTION_COUNT * 2:
            raise ValueError("评估必须包含20截面及两策略")
        keys = {(row.section.as_of, row.strategy) for row in self.sections}
        if len(keys) != SECTION_COUNT * 2 or len({day for day, _ in keys}) != SECTION_COUNT:
            raise ValueError("评估截面或策略重复")
        for row in self.sections:
            if row.section.partition == "sealed" and row.outcomes is not None:
                raise ValueError("封存截面不能公开表现")
            if row.outcomes is not None and set(row.outcomes) != set(row.pool):
                raise ValueError("观测股票与原量化池不一致")
        return self


def candidate_sections(inputs: EvaluationInput) -> dict[date, float]:
    """只凭历史基准完整性和过去SPY收益确定抽样，不窥看个股未来表现。"""
    prices = price_index(inputs.bars)
    sessions = sessions_ending(inputs.cutoff, EVALUATION_HISTORY)
    result = {}
    for index in range(EVALUATION_WARMUP - 1, len(sessions) - LONGEST_HORIZON):
        day = sessions[index]
        required = [
            ("benchmark", code, session)
            for code in BENCHMARKS
            for session in sessions[index - EVALUATION_WARMUP + 1 : index + 1]
        ]
        if all(key in prices for key in required):
            result[day] = (
                prices["benchmark", "SPY", day].close
                / prices["benchmark", "SPY", sessions[index - LONGEST_HORIZON]].close
                - 1
            ) * 100
    return result


def evaluation_versions(
    inputs: EvaluationInput, *, allow_current: bool
) -> tuple[UniverseVersion, ClassificationVersion]:
    """回溯视图另起版本，原始成员有效期和来源全文保存在输入中。"""
    if not allow_current:
        return inputs.universe, inputs.classifications
    first = sessions_ending(inputs.cutoff, EVALUATION_HISTORY)[0]
    active = inputs.universe.active(inputs.cutoff)
    universe = UniverseVersion(
        version=inputs.universe.version + ":evaluation-current",
        create_time=inputs.universe.create_time,
        members=tuple(
            member.model_copy(update={"effective_date": first, "expiry_date": None})
            for members in active.values()
            for member in members
        ),
        limitations=inputs.universe.limitations + (BACKFILL_LIMITATION,),
    )
    classifications = ClassificationVersion(
        version=inputs.classifications.version + ":evaluation-current",
        records=tuple(
            row.model_copy(update={"effective_date": first})
            for row in inputs.classifications.records
            if row.symbol in active and row.effective_date <= inputs.cutoff
        ),
        limitations=inputs.classifications.limitations + (BACKFILL_LIMITATION,),
    )
    return universe, classifications


def section_issues(issues: tuple[QualityIssue, ...], start: date, end: date) -> tuple[QualityIssue, ...]:
    """上游隔离原因携带日期；未来隔离不能提前改变过去截面的分母。"""
    selected = []
    for issue in issues:
        try:
            day = date.fromisoformat(issue.reason.rsplit(":", 1)[-1])
        except ValueError:
            selected.append(issue)
        else:
            if start <= day <= end:
                selected.append(issue)
    return tuple(selected)


def evaluate_section(
    inputs: EvaluationInput,
    section: EvaluationSection,
    versions: tuple[UniverseVersion, ClassificationVersion],
    parameters: tuple[QuantParameters, ...],
    root: Path,
    prices: dict[tuple[str, str, date], Bar],
    coverage_min: float,
) -> tuple[SectionAnalysis, ...]:
    """先截断价格和隔离原因再计算，完整生产证据用于后续人工复核。"""
    snapshot = build_snapshot(
        *versions,
        inputs.bars,
        inputs.source_identity,
        section.as_of,
        coverage_min=coverage_min,
        source_issues=section_issues(
            inputs.issues, sessions_ending(section.as_of, EVALUATION_WARMUP)[0], section.as_of
        ),
    )
    if not snapshot.quality.passed:
        raise ValueError(f"截面{section.as_of}质量未通过：覆盖={snapshot.quality.coverage:.4f}；不能凑足样本")
    binding = root / f"{section.as_of}-snapshot.json"
    if binding.exists():
        saved = SectionBinding.model_validate_json(binding.read_text(encoding="utf-8"))
        if saved.content_hash != snapshot.content_hash:
            raise ValueError("历史截面绑定已改变")
        snapshot = snapshot.model_copy(update={"id": str(saved.id)})
    else:
        write_once(binding, json.dumps({"id": snapshot.id, "content_hash": snapshot.content_hash}))
    result = []
    for config in parameters:
        ensure_source_current()
        strategy = "martin" if isinstance(config, MartinParameters) else "wyckoff"
        filename = f"{section.as_of}-{strategy}.json"
        path = root / filename
        if path.exists():
            analysis = verify_analysis(path.read_text(encoding="utf-8"), snapshot, config)
        else:
            analysis = compute_analysis(snapshot, config)
            write_once(path, analysis.model_dump_json() + "\n")
        outcomes = (
            None
            if section.partition == "sealed"
            else {symbol: indexed_outcomes(symbol, section.as_of, inputs.cutoff, prices) for symbol in analysis.pool}
        )
        result.append(
            SectionAnalysis(
                section=section,
                strategy=strategy,
                snapshot_hash=snapshot.content_hash,
                parameter_hash=analysis.parameter_hash,
                analysis_hash=analysis.content_hash,
                analysis_file=filename,
                universe_count=snapshot.quality.universe_count,
                eligible_count=len(snapshot.quality.eligible[strategy]),
                observation_count=len(analysis.observations),
                exclusions=dict(Counter(str(row.get("reason", "unspecified")) for row in analysis.excluded)),
                pool=analysis.pool,
                outcomes=outcomes,
            )
        )
    return tuple(result)


def evaluate_history(
    inputs: EvaluationInput,
    martin: MartinParameters,
    wyckoff: WyckoffParameters,
    root: Path,
    *,
    allow_current_universe: bool = False,
    coverage_min: float = 0.95,
    timeout_seconds: float = 3000,
) -> EvaluationReport:
    """只运行基线；封存组标签不输出，不自动发布实验参数。"""
    ensure_source_current()
    candidates = candidate_sections(inputs)
    sections = sample_sections(candidates, inputs.cutoff)
    versions = evaluation_versions(inputs, allow_current=allow_current_universe)
    prices, results = price_index(inputs.bars), []
    deadline = time.monotonic() + timeout_seconds
    for section in sections:
        if time.monotonic() >= deadline:
            raise TimeoutError("历史评估已达到总预算")
        results.extend(evaluate_section(inputs, section, versions, (martin, wyckoff), root, prices, coverage_min))
        if time.monotonic() >= deadline:
            raise TimeoutError("历史评估已达到总预算")
    ensure_source_current()
    content = dict(
        schema_version=1,
        kind="historical_quant_pool",
        input_hash=digest(inputs.model_dump(mode="json")),
        source_hash=LOADED_SOURCE_IDENTITY,
        parameter_hashes={
            "martin": digest(martin.model_dump(mode="json")),
            "wyckoff": digest(wyckoff.model_dump(mode="json")),
        },
        cutoff=inputs.cutoff.isoformat(),
        allow_current_universe=allow_current_universe,
        candidate_count=len(candidates),
        sections=[item.model_dump(mode="json") for item in results],
        usable_sections=dict(Counter(item.partition for item in sections if not item.purged)),
        model_selection=None,
        manual_review="pending",
        limitations=(
            "仅量化池回放，不包含模型正式入选或委托成交；每策略10入选/10排除人工审查待完成",
            "当前修订历史不保证严格时点价格；成分与分类的原始版本和取得时间保留在输入",
            "收益是参考收盘价观测，成本复用美股工程估算，不是实际成交费用；不代表策略有效",
            "20个截面且标签净化后样本有限，仅描述性记录；封存组不输出表现；尚未运行参数网格或AKQuant整批比较",
            *((BACKFILL_LIMITATION,) if allow_current_universe else ()),
        ),
    )
    return EvaluationReport(**content, content_hash=digest(content))
