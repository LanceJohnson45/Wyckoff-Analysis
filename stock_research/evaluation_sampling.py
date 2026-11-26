"""按SPY环境分层抽样，再以时间划分并清除跨界收益标签。"""

import math
from datetime import date
from typing import Literal

from stock_research.errors import EvaluationHistoryInsufficient
from stock_research.evaluation_outcomes import observation_sessions
from stock_research.models import FrozenModel

SECTION_COUNT = 20
RESEARCH_END = SECTION_COUNT * 60 // 100
VALIDATION_END = SECTION_COUNT * 80 // 100


class EvaluationSection(FrozenModel):
    """抽样所依据的是历史环境，不是股票未来收益。"""

    as_of: date
    spy_return_20_pct: float
    environment_rank: int
    partition: Literal["research", "validation", "sealed"]
    label_end: date
    purged: bool


def sample_sections(candidates: dict[date, float], cutoff: date) -> tuple[EvaluationSection, ...]:
    """包含强弱两端的20个等距秩，收益同值以日期稳定排序。"""
    if len(candidates) < SECTION_COUNT:
        raise EvaluationHistoryInsufficient("完整行情截面不足20个，不能宣称已完成20截面评估")
    if any(not math.isfinite(value) for value in candidates.values()):
        raise ValueError("SPY环境收益必须为有限数值")
    ranked = sorted(candidates, key=lambda day: (candidates[day], day))
    selected = sorted(
        (ranked[i * (len(ranked) - 1) // (SECTION_COUNT - 1)], i * (len(ranked) - 1) // (SECTION_COUNT - 1))
        for i in range(SECTION_COUNT)
    )
    result = []
    for index, (day, rank) in enumerate(selected):
        end = observation_sessions(day)[-1]
        if end > cutoff:
            raise ValueError("截面最长观察窗口尚未结束")
        partition = "research" if index < RESEARCH_END else "validation" if index < VALIDATION_END else "sealed"
        boundary = (
            selected[RESEARCH_END][0]
            if index < RESEARCH_END
            else selected[VALIDATION_END][0]
            if index < VALIDATION_END
            else None
        )
        result.append(
            EvaluationSection(
                as_of=day,
                spy_return_20_pct=candidates[day],
                environment_rank=rank,
                partition=partition,
                label_end=end,
                purged=boundary is not None and end >= boundary,
            )
        )
    return tuple(result)
