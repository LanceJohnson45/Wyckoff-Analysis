"""Martin两类回调的可复核量化观察，不推断分钟触发或成交。"""

from typing import Literal

import pandas as pd

from stock_research.factors import finite
from stock_research.models import FrozenModel
from stock_research.parameters import MartinParameters


class BranchObservation(FrozenModel):
    """每分支保存原始事实、采用支撑和条件作用，不强制补足候选数。"""

    branch: Literal["mature", "early"]
    qualified: bool
    facts: dict[str, bool]
    excluded_by: tuple[str, ...]
    warnings: tuple[str, ...]
    supports: tuple[dict[str, object], ...]
    support: dict[str, object] | None
    structure: dict[str, object]
    integrity: float
    support_proximity: float | None


def support_candidates(
    frame: pd.DataFrame, parameters: MartinParameters, *, platform: float | None = None
) -> tuple[dict[str, object], ...]:
    """仅采用收盘下方支撑；同距离时平台、EMA中线、EMA快线依次优先。"""
    latest = frame.iloc[-1]
    levels = [("platform", platform), ("ema_middle", latest["ema_middle"]), ("ema_fast", latest["ema_fast"])]
    result = []
    for priority, (kind, value) in enumerate(levels):
        if value is None:
            continue
        distance = abs(latest["low"] - value) / latest["atr"]
        rising = kind == "platform" or platform is None or value > frame.iloc[-parameters.slope_window - 1][kind]
        valid = bool(value <= latest["close"] and distance <= parameters.support_atr and rising)
        result.append(
            {
                "kind": kind,
                "price": finite(value),
                "distance_atr": finite(distance),
                "valid": valid,
                "rising": bool(rising),
                "priority": priority,
                "close_distance": finite(latest["close"] - value),
            }
        )
    return tuple(sorted(result, key=lambda point: (not point["valid"], point["close_distance"], point["priority"])))


def observe_branch(
    branch: Literal["mature", "early"],
    facts: dict[str, bool],
    supports: tuple[dict[str, object], ...],
    structure: dict[str, object],
    parameters: MartinParameters,
) -> BranchObservation:
    """把条件真值和启用/排除/预警分开，完整保留被禁用条件的事实。"""
    excluded, warnings = [], []
    for name, passed in facts.items():
        policy = parameters.rules[name]
        if policy.enabled and not passed:
            if policy.action == "exclude":
                excluded.append(name)
            elif policy.action == "warn":
                warnings.append(name)
    selected = next((support for support in supports if support["valid"]), None)
    proximity = max(0.0, 1 - selected["distance_atr"] / parameters.support_atr) if selected else None
    necessary = [passed for name, passed in facts.items() if name != "contraction"]
    return BranchObservation(
        branch=branch,
        qualified=not excluded,
        facts=facts,
        excluded_by=tuple(excluded),
        warnings=tuple(warnings),
        supports=supports,
        support=selected,
        structure=structure,
        integrity=sum(necessary) / len(necessary),
        support_proximity=proximity,
    )


def mature_observation(frame: pd.DataFrame, parameters: MartinParameters) -> BranchObservation:
    """成熟趋势回调沿用旧工作台完整趋势与回调布尔定义。"""
    latest, previous = frame.iloc[-1], frame.iloc[-2]
    supports = support_candidates(frame, parameters)
    pullback = latest["close"] < frame["close"].iloc[-parameters.pullback_window - 1 : -1].max() or (
        latest["low"] < previous["low"] and latest["close"] < previous["high"]
    )
    facts = {
        "trend": bool(latest["trend"]),
        "pullback": bool(pullback),
        "support": any(support["valid"] for support in supports),
        "extension": bool(latest["extension"] <= parameters.extension_atr),
        "contraction": bool(latest["range_ratio"] <= parameters.contraction_ratio and latest["volume_ratio"] <= 1),
    }
    return observe_branch(
        "mature",
        facts,
        supports,
        {
            "invalidation_reference": float(frame["low"].tail(parameters.pullback_window).min()),
            "daily_high_reference": float(latest["high"]),
            "minute_trigger_verified": False,
        },
        parameters,
    )


def early_observation(frame: pd.DataFrame, parameters: MartinParameters) -> BranchObservation:
    """突破上沿在事件日固定；失效、首次触带及重复回测按历史逐日判定。"""
    latest = frame.iloc[-1]
    candidates = frame.iloc[-parameters.breakout_recency : -1]
    events = candidates.index[candidates["breakout"]].tolist()
    event = events[-1] if events else None
    level = float(frame.loc[event, "breakout_level"]) if event is not None else None
    supports = support_candidates(frame, parameters, platform=level) if event is not None else ()
    selected = next((support for support in supports if support["valid"]), None)
    after = frame.loc[event + 1 :] if event is not None else frame.iloc[0:0]
    invalidated = (
        bool((after["close"] < level - parameters.support_atr * after["atr"]).any()) if level is not None else True
    )
    episodes = retest_episodes(after, selected, level, parameters) if selected is not None else 0
    facts = {
        "early_structure": event is not None and not invalidated,
        "pullback": bool(event is not None and latest["close"] < frame.loc[event:, "close"].max()),
        "support": selected is not None,
        "first_retest": episodes == 1,
        "extension": bool(latest["extension"] <= parameters.extension_atr),
        "contraction": bool(latest["range_ratio"] <= parameters.contraction_ratio and latest["volume_ratio"] <= 1),
    }
    structure = {
        "breakout_date": str(frame.loc[event, "date"]) if event is not None else None,
        "platform": level,
        "invalidated": invalidated,
        "retest_episodes": episodes,
        "daily_high_reference": float(latest["high"]),
        "minute_trigger_verified": False,
    }
    return observe_branch("early", facts, supports, structure, parameters)


def retest_episodes(
    after: pd.DataFrame, support: dict[str, object], platform: float, parameters: MartinParameters
) -> int:
    """连续触带日归为同一次，离带后再次触带记为重复回测。"""
    kind = support["kind"]
    levels = pd.Series(platform, index=after.index) if kind == "platform" else after[kind]
    touched = ((after["low"] - levels).abs() <= parameters.support_atr * after["atr"]) & (after["close"] >= levels)
    return int((touched & ~touched.shift(fill_value=False)).sum())
