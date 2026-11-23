"""快照内Martin量化扫描、解释性排序与不可变观察产物。"""

from collections import defaultdict

from pydantic import AwareDatetime, Field, model_validator

from stock_research.factors import FACTOR_DEFINITIONS, anchor_evidence, factor_frame, latest_factors
from stock_research.groups import group_facts, percentiles
from stock_research.identity import digest, now
from stock_research.martin import BranchObservation, early_observation, mature_observation
from stock_research.models import FrozenModel, Snapshot
from stock_research.parameters import MartinParameters


class MartinObservation(FrozenModel):
    """一只股票的事实与两个分支，观察参考不能视为交易指令。"""

    symbol: str
    factors: dict[str, float | None]
    branches: tuple[BranchObservation, ...]
    anchors: tuple[dict[str, object], ...]
    score_components: dict[str, float | None] = Field(default_factory=dict)
    branch_scores: dict[str, float] = Field(default_factory=dict)


class MartinReport(FrozenModel):
    """本地计算结果，不包含模型复核、正式初选或执行结论。"""

    schema_version: int
    snapshot_id: str
    snapshot_hash: str
    as_of: str
    parameters: MartinParameters
    factor_definitions: dict[str, str]
    parameter_hash: str
    create_time: AwareDatetime
    universe_count: int
    observations: tuple[MartinObservation, ...]
    groups: tuple[dict[str, object], ...]
    excluded: tuple[dict[str, object], ...]
    pool: tuple[str, ...]
    model_selection: None = None
    limitations: tuple[str, ...]
    content_hash: str

    @model_validator(mode="after")
    def verify_content(self) -> "MartinReport":
        """读写均校验完整结果，不能用损坏的本地缓存冒充新计算。"""
        if (
            self.schema_version != 1
            or digest(self.model_dump(mode="json", exclude={"content_hash"})) != self.content_hash
        ):
            raise ValueError("Martin观察产物的版本或内容摘要不匹配")
        if len(set(self.pool)) != len(self.pool) or len(self.pool) > self.parameters.pool_size:
            raise ValueError("Martin量化池存在重复或超过上限")
        return self


def compute_martin(snapshot: Snapshot, parameters: MartinParameters) -> MartinReport:
    """只计算质量通过快照；历史不足和必要值缺失逐股列示。"""
    if not snapshot.quality.passed:
        raise ValueError("数据质量未通过，不生成Martin量化池")
    bars = defaultdict(list)
    for bar in snapshot.bars:
        bars[bar.kind, bar.symbol].append(bar)
    benchmark = factor_frame(tuple(bars["benchmark", "SPY"]), parameters)
    observations, trends = [], {}
    excluded = [
        issue.model_dump(mode="json") for issue in snapshot.quality.issues if issue.strategy in {"all", "martin"}
    ]
    minimum = max(
        100,
        parameters.ema_periods[-1] * 2,
        parameters.atr_window,
        parameters.adr_window,
        parameters.rs_window + 1,
        parameters.recent_window + parameters.base_window,
        parameters.breakout_window + parameters.breakout_recency,
        parameters.pullback_window + 1,
        parameters.slope_window + 1,
    )
    for symbol in snapshot.quality.eligible["martin"]:
        daily = tuple(bars["stock", symbol])
        if len(daily) < minimum:
            excluded.append({"symbol": symbol, "reason": f"parameter_warmup:{minimum}"})
            continue
        frame = factor_frame(daily, parameters)
        facts = latest_factors(frame, benchmark, parameters)
        if facts["atr"] is None or facts["atr"] <= 0 or facts["volume"] <= 0 or facts["rs"] is None:
            excluded.append({"symbol": symbol, "reason": "invalid_atr_volume_or_aligned_benchmark"})
            continue
        branches = (mature_observation(frame, parameters), early_observation(frame, parameters))
        trends[symbol] = bool(frame.iloc[-1]["trend"])
        observations.append(
            MartinObservation(
                symbol=symbol, factors=facts, branches=branches, anchors=anchor_evidence(frame, parameters)
            )
        )
    groups = group_facts(snapshot, {item.symbol: item.factors for item in observations}, trends, parameters)
    scored = score_observations(tuple(observations), groups, parameters)
    pool = merge_pool(scored, parameters.pool_size)
    content = dict(
        schema_version=1,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        as_of=snapshot.as_of.isoformat(),
        parameters=parameters.model_dump(mode="json"),
        factor_definitions=FACTOR_DEFINITIONS,
        parameter_hash=digest(parameters.model_dump(mode="json")),
        create_time=now().isoformat(),
        universe_count=snapshot.quality.universe_count,
        observations=[item.model_dump(mode="json") for item in scored],
        groups=groups,
        excluded=tuple(excluded),
        pool=pool,
        model_selection=None,
        limitations=snapshot.quality.limitations
        + (
            "量化观察池不是模型初选或买入指令；未校验分钟触发、成交与交易成本绩效",
            "ATR尺度、支撑带、结构与排名权重均属工程研究定义，未做参数优化或历史收益验证",
            "金额代理缺失：尚未确认复权价格与成交量单位兼容，不使用其替代真实金额门槛",
            "数值使用pandas参考计算；AKQuant未经逐值对齐的算子未替换生产路径",
        ),
    )
    return MartinReport(**content, content_hash=digest(content))


def score_observations(
    observations: tuple[MartinObservation, ...], groups: tuple[dict[str, object], ...], parameters: MartinParameters
) -> tuple[MartinObservation, ...]:
    """排名基于本次有效横截面；缺辅助项只对可用权重重归一化。"""
    percent = {
        key: percentiles({item.symbol: item.factors[key] for item in observations})
        for key in ("rs", "volume_ratio", "range_ratio")
    }
    result = []
    for item in observations:
        group_scores = [
            group["score"] for group in groups if item.symbol in group["valid_members"] and group["score"] is not None
        ]
        components = {
            "rs": percent["rs"].get(item.symbol),
            "group": max(group_scores) if group_scores else None,
            "volume": 1 - percent["volume_ratio"][item.symbol] if item.symbol in percent["volume_ratio"] else None,
            "contraction": 1 - percent["range_ratio"][item.symbol] if item.symbol in percent["range_ratio"] else None,
        }
        scores = {}
        unscored = set()
        for branch in item.branches:
            values = {**components, "support": branch.support_proximity, "integrity": branch.integrity}
            if not parameters.rules["contraction"].enabled:
                values["contraction"] = None
            denominator = sum(parameters.weights[key] for key, value in values.items() if value is not None)
            if not denominator:
                unscored.add(branch.branch)
            scores[branch.branch] = (
                sum(parameters.weights[key] * value for key, value in values.items() if value is not None)
                / denominator
                * 100
                if denominator
                else 0.0
            )
        branches = tuple(
            branch.model_copy(
                update={"qualified": False, "excluded_by": (*branch.excluded_by, "no_available_score_weight")}
            )
            if branch.branch in unscored
            else branch
            for branch in item.branches
        )
        result.append(
            item.model_copy(update={"score_components": components, "branch_scores": scores, "branches": branches})
        )
    return tuple(result)


def merge_pool(observations: tuple[MartinObservation, ...], limit: int) -> tuple[str, ...]:
    """两个分支分别排序，按名次轮流取并去重，不以配额补足。"""
    ranked = {
        branch: sorted(
            (
                item
                for item in observations
                if any(candidate.branch == branch and candidate.qualified for candidate in item.branches)
            ),
            key=lambda item: (-item.branch_scores[branch], item.symbol),
        )
        for branch in ("early", "mature")
    }
    result = []
    for rank in range(max((len(items) for items in ranked.values()), default=0)):
        for branch in ("early", "mature"):
            if rank < len(ranked[branch]) and ranked[branch][rank].symbol not in result:
                result.append(ranked[branch][rank].symbol)
                if len(result) == limit:
                    return tuple(result)
    return tuple(result)
