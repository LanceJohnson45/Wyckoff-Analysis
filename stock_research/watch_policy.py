"""原量化事实的观察状态投影，不计算替代信号或改动生产参数。"""

from dataclasses import dataclass

from stock_research.analysis import MartinReport
from stock_research.wyckoff import WyckoffReport


@dataclass(frozen=True)
class WatchAssessment:
    available: bool
    reviewed: bool
    valid: bool
    near: bool
    reason: str
    refs: tuple[str, ...]


def assess_martin(analysis: MartinReport, symbol: str) -> WatchAssessment:
    """只用已保存的分支事实与规则作用；回调/延伸离带不自动结束结构。"""
    observation = next((row for row in analysis.observations if row.symbol == symbol), None)
    if observation is None:
        return WatchAssessment(False, False, False, False, "calculation_missing", ())
    surviving, refs = [], []
    for index, branch in enumerate(observation.branches):
        structural = {"trend", "early_structure", "first_retest"} & set(branch.facts)
        refs.extend(f"martin/{symbol}/branches/{index}/facts/{key}" for key in sorted(structural))
        if not structural.intersection(branch.excluded_by):
            surviving.append((index, branch))
    near = any(branch.facts.get("support", False) for _, branch in surviving)
    refs.extend(f"martin/{symbol}/branches/{index}/facts/support" for index, _ in surviving)
    return WatchAssessment(
        True, True, bool(surviving), near, "structure_valid" if surviving else "structure_invalid", tuple(refs)
    )


def assess_wyckoff(analysis: WyckoffReport, symbol: str) -> WatchAssessment:
    """L1未到结构层时保留旧状态；水温/无触发不冒充结构失效。"""
    row = next((row for row in analysis.observations if row["symbol"] == symbol), None)
    if row is None:
        return WatchAssessment(False, False, False, False, "calculation_missing", ())
    if not row["layer1"]:
        return WatchAssessment(True, False, False, False, "production_layer1_not_passed", (f"wyckoff/{symbol}/layer1",))
    candidate = next((row for row in analysis.candidates if row["symbol"] == symbol), None)
    touches = candidate.get("springboard_evidence", {}).get("c_support", {}).get("touch_dates", []) if candidate else []
    near = str(analysis.as_of) in {str(day)[:10] for day in touches}
    refs = [f"wyckoff/{symbol}/layer2"]
    if near:
        refs.extend(
            f"wyckoff/{symbol}/candidate/springboard_evidence/c_support/touch_dates/{index}"
            for index, day in enumerate(touches)
            if str(day)[:10] == str(analysis.as_of)
        )
    return WatchAssessment(
        True,
        True,
        bool(row["layer2"]),
        near,
        "structure_valid" if row["layer2"] else "production_layer2_not_passed",
        tuple(refs),
    )
