"""行业和已确认主题的覆盖、强度事实及横截面分位。"""

from statistics import median

import pandas as pd

from stock_research.models import Snapshot
from stock_research.parameters import MartinParameters


def percentiles(values: dict[str, float | None]) -> dict[str, float]:
    """相同值取平均名次；单一样本置中，不把缺失值填成零分。"""
    available = {key: value for key, value in values.items() if value is not None}
    if len(available) <= 1:
        return {key: 0.5 for key in available}
    ranked = (pd.Series(available).rank(method="average") - 1) / (len(available) - 1)
    return {key: float(value) for key, value in ranked.items()}


def group_facts(
    snapshot: Snapshot,
    factors: dict[str, dict[str, float | None]],
    trends: dict[str, bool],
    parameters: MartinParameters,
) -> tuple[dict[str, object], ...]:
    """分母保留完整组成员；少于3只或低覆盖的组仅展示，不产生分数。"""
    active = set(snapshot.universe.active(snapshot.as_of))
    groups: dict[str, set[str]] = {}
    for record in snapshot.classifications.records:
        if record.symbol not in active or record.effective_date > snapshot.as_of:
            continue
        keys = (["industry:" + record.industry] if record.industry else []) + [
            "theme:" + theme for theme in record.themes
        ]
        for key in keys:
            groups.setdefault(key, set()).add(record.symbol)
    result = []
    for name, member_set in sorted(groups.items()):
        members = sorted(member_set)
        valid = [
            symbol
            for symbol in members
            if symbol in factors and all(factors[symbol][key] is not None for key in ("rs", "return_1", "return_3"))
        ]
        coverage = len(valid) / len(members)
        usable = len(valid) >= parameters.min_group_members and coverage >= parameters.group_coverage_min
        result.append(
            {
                "name": name,
                "members": members,
                "valid_members": valid,
                "coverage": coverage,
                "usable": usable,
                "rs_median": median(factors[symbol]["rs"] for symbol in valid) if valid else None,
                "up_1": sum(factors[symbol]["return_1"] > 0 for symbol in valid) / len(valid) if valid else None,
                "up_3": sum(factors[symbol]["return_3"] > 0 for symbol in valid) / len(valid) if valid else None,
                "trend_health": sum(trends[symbol] for symbol in valid) / len(valid) if valid else None,
            }
        )
    components = {
        key: percentiles({group["name"]: (group[key] if group["usable"] else None) for group in result})
        for key in ("rs_median", "up_1", "up_3", "trend_health")
    }
    for group in result:
        name = group["name"]
        group["score"] = (
            (
                components["rs_median"][name]
                + (components["up_1"][name] + components["up_3"][name]) / 2
                + components["trend_health"][name]
            )
            / 3
            if group["usable"]
            else None
        )
    return tuple(result)
