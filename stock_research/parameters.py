"""Martin日线研究参数及因子、公式、阈值的独立出处。"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from stock_research.models import FrozenModel

Period = Annotated[int, Field(ge=1, le=1000)]


class RulePolicy(FrozenModel):
    """开关和作用方式必须随运行冻结，未确认阈值明确标工程定义。"""

    enabled: bool = True
    action: Literal["exclude", "bonus", "warn"] = "exclude"
    concept_source: str
    formula_source: str = "工程研究定义，未确认属于Martin统一公式"
    parameter_source: str = "工程默认值，未经参数优化或收益验证"


def default_rules() -> dict[str, RulePolicy]:
    """访谈概念依据与旧工作台工程组合分开保存。"""
    return {
        "trend": RulePolicy(concept_source="本人访谈00:25:59-00:26:30的EMA9/21/50；完整多头组合为工程定义"),
        "pullback": RulePolicy(concept_source="已有强势回调观察；固定识别窗口为工程定义"),
        "support": RulePolicy(concept_source="访谈支撑反应；ATR带宽为工程定义"),
        "extension": RulePolicy(concept_source="旧工作台延伸预警；ATR倍数为工程定义"),
        "contraction": RulePolicy(action="bonus", concept_source="已有收紧、缩量观察；窗口和比例为工程定义"),
        "early_structure": RulePolicy(concept_source="早期转强回调为工程研究分支，非已验证统一扫描公式"),
        "first_retest": RulePolicy(concept_source="首次回测状态算法为工程研究定义"),
    }


class MartinParameters(FrozenModel):
    """所有数值改变都形成新参数摘要，拒绝拼写错误与未知条件。"""

    version: str = Field(default="baseline-v1", min_length=1, max_length=256, pattern=r"^[^\x00]+$")
    algorithm: Literal["martin-daily-v1"] = "martin-daily-v1"
    ema_periods: tuple[Period, Period, Period] = (9, 21, 50)
    slope_window: Period = 5
    atr_window: Period = 14
    adr_window: Period = 20
    rs_window: Period = 20
    recent_window: Period = 3
    base_window: Period = 10
    pullback_window: Period = 5
    support_atr: float = Field(default=0.75, gt=0)
    extension_atr: float = Field(default=3, gt=0)
    contraction_ratio: float = Field(default=0.7, gt=0, le=1)
    breakout_window: Period = 60
    breakout_recency: Period = 20
    swing_left: Period = 3
    swing_right: Period = 3
    anchor_window: Period = 60
    pool_size: int = Field(default=30, ge=1, le=100)
    min_group_members: int = Field(default=3, ge=3)
    group_coverage_min: float = Field(default=0.8, gt=0, le=1)
    weights: dict[str, float] = Field(
        default_factory=lambda: {
            "rs": 0.25,
            "group": 0.20,
            "support": 0.20,
            "integrity": 0.15,
            "volume": 0.10,
            "contraction": 0.10,
        }
    )
    rules: dict[str, RulePolicy] = Field(default_factory=default_rules)

    @model_validator(mode="after")
    def validate_parameters(self) -> "MartinParameters":
        """零权重总和、错规则名和倒置尺度必须在计算前拒绝。"""
        if tuple(sorted(set(self.ema_periods))) != self.ema_periods:
            raise ValueError("EMA周期须严格递增")
        if self.support_atr >= self.extension_atr:
            raise ValueError("观察带宽须小于延伸距离")
        if set(self.rules) != set(default_rules()):
            raise ValueError("规则集合不完整或存在未知规则")
        if set(self.weights) != {"rs", "group", "support", "integrity", "volume", "contraction"}:
            raise ValueError("评分权重名称不完整或未知")
        if any(not 0 <= value <= 1 for value in self.weights.values()) or sum(self.weights.values()) <= 0:
            raise ValueError("评分权重须非负且总和为正")
        return self
