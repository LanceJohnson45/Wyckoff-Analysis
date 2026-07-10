"""Helpers for comparing shadow stock-selection strategy outputs."""

from __future__ import annotations

from dataclasses import dataclass


TRIGGER_LABELS = {
    "sos": "SOS（强势信号）",
    "spring": "Spring（假跌破修复）",
    "lps": "LPS（最后支撑点）",
    "evr": "EVR（放量不跌）",
}


@dataclass(frozen=True)
class StrategyCandidate:
    strategy_id: str
    code: str
    triggers: tuple[str, ...]
    score: float
    stage: str = ""
    channel: str = ""


@dataclass(frozen=True)
class StrategyRun:
    strategy_id: str
    result: object | None
    candidates: tuple[StrategyCandidate, ...]


@dataclass(frozen=True)
class StrategyComparison:
    strategy_ids: tuple[str, ...]
    intersection: tuple[str, ...]
    union: tuple[str, ...]
    only_by_strategy: dict[str, tuple[str, ...]]
    counts: dict[str, int]


def extract_l4_candidates(result, strategy_id: str) -> tuple[StrategyCandidate, ...]:
    trigger_map: dict[str, list[str]] = {}
    score_map: dict[str, float] = {}
    triggers = getattr(result, "triggers", {}) or {}
    stage_map = getattr(result, "stage_map", {}) or {}
    channel_map = getattr(result, "channel_map", {}) or {}
    for trigger_key, pairs in triggers.items():
        for code, score in pairs:
            code_s = str(code).strip()
            if not code_s:
                continue
            trigger_map.setdefault(code_s, []).append(str(trigger_key))
            score_map[code_s] = score_map.get(code_s, 0.0) + float(score)

    out: list[StrategyCandidate] = []
    for code in sorted(trigger_map, key=lambda c: (-score_map.get(c, 0.0), c)):
        out.append(
            StrategyCandidate(
                strategy_id=strategy_id,
                code=code,
                triggers=tuple(trigger_map.get(code, [])),
                score=float(score_map.get(code, 0.0)),
                stage=str(stage_map.get(code, "") or ""),
                channel=str(channel_map.get(code, "") or ""),
            )
        )
    return tuple(out)


def compare_strategy_runs(runs: list[StrategyRun] | tuple[StrategyRun, ...]) -> StrategyComparison:
    if not runs:
        return StrategyComparison(strategy_ids=(), intersection=(), union=(), only_by_strategy={}, counts={})

    strategy_ids = tuple(run.strategy_id for run in runs)
    code_sets = {run.strategy_id: {candidate.code for candidate in run.candidates} for run in runs}
    all_codes = set().union(*code_sets.values()) if code_sets else set()
    intersection = set.intersection(*code_sets.values()) if code_sets else set()
    only_by_strategy: dict[str, tuple[str, ...]] = {}
    for strategy_id, codes in code_sets.items():
        other_codes = set().union(*(v for k, v in code_sets.items() if k != strategy_id))
        only_by_strategy[strategy_id] = tuple(sorted(codes - other_codes))
    counts = {"union": len(all_codes), "intersection": len(intersection)}
    for strategy_id, codes in code_sets.items():
        counts[strategy_id] = len(codes)
        counts[f"only_{strategy_id}"] = len(only_by_strategy[strategy_id])
    return StrategyComparison(
        strategy_ids=strategy_ids,
        intersection=tuple(sorted(intersection)),
        union=tuple(sorted(all_codes)),
        only_by_strategy=only_by_strategy,
        counts=counts,
    )


__all__ = [
    "TRIGGER_LABELS",
    "StrategyCandidate",
    "StrategyRun",
    "StrategyComparison",
    "extract_l4_candidates",
    "compare_strategy_runs",
]
