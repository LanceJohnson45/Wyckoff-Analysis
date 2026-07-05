# -*- coding: utf-8 -*-
"""
通用日线指标规则引擎。

目标：
- 读取 query/ 下的 JSON 指标定义
- 基于日线 O/H/L/C/V 序列计算数值序列、事件与 latest_match
- 返回足够的中间结果，方便校验提取规则是否可执行
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import pandas as pd

BASE_FIELDS = {"open", "high", "low", "close", "volume"}


@dataclass(frozen=True)
class RuleMatch:
    start_idx: int
    end_idx: int
    event_days: dict[str, int]


@dataclass(frozen=True)
class IndicatorDataRequirements:
    required_fields: tuple[str, ...]
    min_bars: int
    warmup_bars: int
    evaluation_buffer: int


@dataclass(frozen=True)
class IndicatorEvaluation:
    name: str
    timeframe: str
    latest_match: bool
    latest_index: int | None
    latest_date: Any | None
    signal_date: Any | None
    required_fields_ok: bool
    missing_fields: tuple[str, ...]
    series_values: dict[str, pd.Series]
    event_values: dict[str, pd.Series]
    matches: tuple[RuleMatch, ...]


def load_indicator_definition(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def infer_data_requirements(
    definition: dict[str, Any],
    *,
    evaluation_buffer: int = 20,
) -> IndicatorDataRequirements:
    required_fields = tuple(definition.get("required_fields", []))
    warmup_bars = max(int(definition.get("warmup_bars", 0) or 0), 0)
    min_bars = max(warmup_bars + max(int(evaluation_buffer), 0), warmup_bars, 1)
    return IndicatorDataRequirements(
        required_fields=required_fields,
        min_bars=min_bars,
        warmup_bars=warmup_bars,
        evaluation_buffer=max(int(evaluation_buffer), 0),
    )


class IndicatorRuleEngine:
    def evaluate(
        self,
        df: pd.DataFrame,
        definition: dict[str, Any],
    ) -> IndicatorEvaluation:
        frame = self._normalize_frame(df)
        params = dict(definition.get("params", {}))
        required_fields = tuple(definition.get("required_fields", []))
        missing_fields = tuple(field for field in required_fields if field not in frame.columns)
        series_map = self._build_series_map(frame, definition, params)
        event_map = self._build_event_map(frame, definition, params, series_map)

        latest_index = len(frame) - 1 if not frame.empty else None
        if latest_index is None or missing_fields:
            return IndicatorEvaluation(
                name=str(definition.get("name", "")),
                timeframe=str(definition.get("timeframe", "1d")),
                latest_match=False,
                latest_index=latest_index,
                latest_date=None if latest_index is None else frame.iloc[latest_index].get("date"),
                signal_date=None,
                required_fields_ok=not missing_fields,
                missing_fields=missing_fields,
                series_values=series_map,
                event_values=event_map,
                matches=tuple(),
            )

        memo: dict[tuple[str, int], list[RuleMatch]] = {}
        latest_rule = definition.get("rules", {}).get("latest_match", {})
        matches = self._evaluate_rule_matches(
            node=latest_rule,
            day_idx=latest_index,
            frame=frame,
            params=params,
            series_map=series_map,
            event_map=event_map,
            memo=memo,
        )
        signal_date = self._resolve_signal_date(frame, matches)
        return IndicatorEvaluation(
            name=str(definition.get("name", "")),
            timeframe=str(definition.get("timeframe", "1d")),
            latest_match=bool(matches),
            latest_index=latest_index,
            latest_date=frame.iloc[latest_index].get("date"),
            signal_date=signal_date,
            required_fields_ok=True,
            missing_fields=tuple(),
            series_values=series_map,
            event_values=event_map,
            matches=tuple(matches),
        )

    def _normalize_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        frame = df.copy()
        if "date" in frame.columns:
            frame = frame.sort_values("date").reset_index(drop=True)
        for field in BASE_FIELDS:
            if field in frame.columns:
                frame[field] = pd.to_numeric(frame[field], errors="coerce")
        return frame

    def _build_series_map(
        self,
        frame: pd.DataFrame,
        definition: dict[str, Any],
        params: dict[str, Any],
    ) -> dict[str, pd.Series]:
        series_map: dict[str, pd.Series] = {}
        for field in BASE_FIELDS:
            if field in frame.columns:
                series_map[field] = pd.to_numeric(frame[field], errors="coerce")

        for item in definition.get("series", []):
            series_id = item["id"]
            op = item["op"]
            field = item["field"]
            window = self._resolve_value(item.get("window"), params)
            series_map[series_id] = self._compute_series(op, field, window, series_map, params)
        return series_map

    def _compute_series(
        self,
        op: str,
        field: Any,
        window: Any,
        series_map: dict[str, pd.Series],
        params: dict[str, Any],
    ) -> pd.Series:
        source = self._resolve_series_reference(field, series_map)
        if op == "sma":
            return source.rolling(int(window), min_periods=int(window)).mean()
        if op == "ema":
            return source.ewm(span=int(window), adjust=False, min_periods=int(window)).mean()
        if op == "highest":
            return source.rolling(int(window), min_periods=int(window)).max()
        if op == "lowest":
            return source.rolling(int(window), min_periods=int(window)).min()
        if op == "sum":
            return source.rolling(int(window), min_periods=int(window)).sum()
        if op == "ref":
            return source.shift(int(window))
        if op == "pct_change":
            return source.pct_change(periods=int(window))
        raise ValueError(f"Unsupported series op: {op}")

    def _build_event_map(
        self,
        frame: pd.DataFrame,
        definition: dict[str, Any],
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
    ) -> dict[str, pd.Series]:
        event_map: dict[str, pd.Series] = {}
        for item in definition.get("events", []):
            event_id = item["id"]
            op = item["op"]
            left = self._resolve_numeric_reference(item["left"], frame, series_map, params)
            right = self._resolve_numeric_reference(item["right"], frame, series_map, params)
            if op == "cross_up":
                event_map[event_id] = (left.shift(1) <= right.shift(1)) & (left > right)
            elif op == "cross_down":
                event_map[event_id] = (left.shift(1) >= right.shift(1)) & (left < right)
            elif op == "breakout_up":
                event_map[event_id] = (left.shift(1) <= right.shift(1)) & (left > right)
            elif op == "breakout_down":
                event_map[event_id] = (left.shift(1) >= right.shift(1)) & (left < right)
            else:
                raise ValueError(f"Unsupported event op: {op}")
            event_map[event_id] = event_map[event_id].fillna(False).astype(bool)
        return event_map

    def _evaluate_rule_matches(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        key = (json.dumps(node, sort_keys=True, ensure_ascii=False), day_idx)
        cached = memo.get(key)
        if cached is not None:
            return cached

        op = node.get("op")
        if op == "condition":
            matches = self._matches_for_condition_node(node, day_idx, event_map)
        elif op == "compare":
            matches = self._matches_for_compare_node(node, day_idx, frame, params, series_map)
        elif op == "and":
            matches = self._matches_for_and_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "or":
            matches = self._matches_for_or_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "not":
            inner = self._evaluate_rule_matches(
                node["condition"], day_idx, frame, params, series_map, event_map, memo
            )
            matches = [RuleMatch(day_idx, day_idx, {})] if not inner else []
        elif op == "within":
            matches = self._matches_for_within_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "count":
            matches = self._matches_for_count_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "consecutive":
            matches = self._matches_for_consecutive_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "sequence":
            matches = self._matches_for_sequence_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        elif op == "before":
            matches = self._matches_for_before_node(
                node, day_idx, frame, params, series_map, event_map, memo
            )
        else:
            raise ValueError(f"Unsupported rule op: {op}")

        memo[key] = matches
        return matches

    def _matches_for_condition_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        event_map: dict[str, pd.Series],
    ) -> list[RuleMatch]:
        event_id = node["event"]
        series = event_map[event_id]
        if bool(series.iloc[day_idx]):
            return [RuleMatch(day_idx, day_idx, {event_id: day_idx})]
        return []

    def _matches_for_compare_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
    ) -> list[RuleMatch]:
        left = self._resolve_numeric_reference(node["left"], frame, series_map, params)
        right = self._resolve_numeric_reference(node["right"], frame, series_map, params)
        operator = node["operator"]
        left_value = left.iloc[day_idx]
        right_value = right.iloc[day_idx]
        if pd.isna(left_value) or pd.isna(right_value):
            return []
        passed = self._apply_operator(float(left_value), operator, float(right_value))
        return [RuleMatch(day_idx, day_idx, {})] if passed else []

    def _matches_for_and_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        merged_matches = [RuleMatch(day_idx, day_idx, {})]
        for child in node.get("conditions", []):
            child_matches = self._evaluate_rule_matches(
                child, day_idx, frame, params, series_map, event_map, memo
            )
            if not child_matches:
                return []
            next_matches: list[RuleMatch] = []
            for left_match in merged_matches:
                for right_match in child_matches:
                    merged = self._merge_matches(left_match, right_match, day_idx)
                    if merged is not None:
                        next_matches.append(merged)
            merged_matches = self._dedupe_matches(next_matches)
            if not merged_matches:
                return []
        return merged_matches

    def _matches_for_or_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        matches: list[RuleMatch] = []
        for child in node.get("conditions", []):
            matches.extend(
                self._evaluate_rule_matches(
                    child, day_idx, frame, params, series_map, event_map, memo
                )
            )
        return self._dedupe_matches(matches)

    def _matches_for_within_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        window = int(self._resolve_value(node["window"], params))
        start = max(0, day_idx - window + 1)
        results: list[RuleMatch] = []
        for idx in range(start, day_idx + 1):
            inner_matches = self._evaluate_rule_matches(
                node["condition"], idx, frame, params, series_map, event_map, memo
            )
            for match in inner_matches:
                results.append(
                    RuleMatch(start_idx=match.start_idx, end_idx=day_idx, event_days=dict(match.event_days))
                )
        return self._dedupe_matches(results)

    def _matches_for_count_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        window = int(self._resolve_value(node["window"], params))
        start = max(0, day_idx - window + 1)
        count = 0
        for idx in range(start, day_idx + 1):
            inner_matches = self._evaluate_rule_matches(
                node["condition"], idx, frame, params, series_map, event_map, memo
            )
            if inner_matches:
                count += 1
        if "gte" in node and count < int(self._resolve_value(node["gte"], params)):
            return []
        if "lte" in node and count > int(self._resolve_value(node["lte"], params)):
            return []
        if "eq" in node and count != int(self._resolve_value(node["eq"], params)):
            return []
        return [RuleMatch(start, day_idx, {})]

    def _matches_for_consecutive_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        required = int(self._resolve_value(node["length"], params))
        count = 0
        idx = day_idx
        while idx >= 0:
            inner_matches = self._evaluate_rule_matches(
                node["condition"], idx, frame, params, series_map, event_map, memo
            )
            if not inner_matches:
                break
            count += 1
            idx -= 1
        if count >= required:
            return [RuleMatch(day_idx - required + 1, day_idx, {})]
        return []

    def _matches_for_sequence_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        window = int(self._resolve_value(node["window"], params))
        allow_same_day = bool(self._resolve_value(node.get("allow_same_day", False), params))
        conditions = list(node.get("conditions", []))
        if not conditions:
            return []

        start = max(0, day_idx - window + 1)
        results: list[RuleMatch] = []
        for last_day in range(start, day_idx + 1):
            results.extend(
                self._sequence_matches_for_completion_day(
                    conditions=conditions,
                    completion_day=last_day,
                    current_day=day_idx,
                    min_day=start,
                    allow_same_day=allow_same_day,
                    frame=frame,
                    params=params,
                    series_map=series_map,
                    event_map=event_map,
                    memo=memo,
                )
            )
        return self._dedupe_matches(results)

    def _sequence_matches_for_completion_day(
        self,
        conditions: list[dict[str, Any]],
        completion_day: int,
        current_day: int,
        min_day: int,
        allow_same_day: bool,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        def walk(step_idx: int, day_limit: int) -> list[RuleMatch]:
            cond = conditions[step_idx]
            matches: list[RuleMatch] = []
            search_start = min_day
            search_end = day_limit
            for day in range(search_start, search_end + 1):
                current_matches = self._evaluate_rule_matches(
                    cond, day, frame, params, series_map, event_map, memo
                )
                if not current_matches:
                    continue
                if step_idx == 0:
                    matches.extend(current_matches)
                    continue
                prev_limit = day if allow_same_day else day - 1
                if prev_limit < min_day:
                    continue
                prev_matches = walk(step_idx - 1, prev_limit)
                for prev_match in prev_matches:
                    for current_match in current_matches:
                        merged = self._merge_matches(prev_match, current_match, current_day)
                        if merged is not None:
                            matches.append(merged)
            return self._dedupe_matches(matches)

        completed_matches = walk(len(conditions) - 1, completion_day)
        output: list[RuleMatch] = []
        for match in completed_matches:
            output.append(
                RuleMatch(
                    start_idx=match.start_idx,
                    end_idx=current_day,
                    event_days=dict(match.event_days),
                )
            )
        return self._dedupe_matches(output)

    def _matches_for_before_node(
        self,
        node: dict[str, Any],
        day_idx: int,
        frame: pd.DataFrame,
        params: dict[str, Any],
        series_map: dict[str, pd.Series],
        event_map: dict[str, pd.Series],
        memo: dict[tuple[str, int], list[RuleMatch]],
    ) -> list[RuleMatch]:
        event_id = node["event"]
        window = int(self._resolve_value(node["window"], params))
        min_count = int(self._resolve_value(node.get("min_count", 1), params))
        results: list[RuleMatch] = []
        for event_day in range(0, day_idx + 1):
            event_matches = self._evaluate_rule_matches(
                {"op": "condition", "event": event_id},
                event_day,
                frame,
                params,
                series_map,
                event_map,
                memo,
            )
            if not event_matches:
                continue
            start = max(0, event_day - window)
            count = 0
            for idx in range(start, event_day):
                cond_matches = self._evaluate_rule_matches(
                    node["condition"], idx, frame, params, series_map, event_map, memo
                )
                if cond_matches:
                    count += 1
            if count >= min_count:
                results.append(
                    RuleMatch(
                        start_idx=start,
                        end_idx=day_idx,
                        event_days={event_id: event_day},
                    )
                )
        return self._dedupe_matches(results)

    def _resolve_signal_date(
        self,
        frame: pd.DataFrame,
        matches: list[RuleMatch],
    ) -> Any | None:
        if not matches:
            return None
        latest_signal_idx = max(
            max(match.event_days.values()) if match.event_days else match.end_idx
            for match in matches
        )
        return frame.iloc[latest_signal_idx].get("date")

    def _resolve_series_reference(
        self,
        field: Any,
        series_map: dict[str, pd.Series],
    ) -> pd.Series:
        if isinstance(field, str) and field in series_map:
            return series_map[field]
        raise KeyError(f"Unknown series reference: {field}")

    def _resolve_numeric_reference(
        self,
        ref: Any,
        frame: pd.DataFrame,
        series_map: dict[str, pd.Series],
        params: dict[str, Any],
    ) -> pd.Series:
        resolved = self._resolve_value(ref, params)
        if isinstance(resolved, str):
            if resolved in series_map:
                return pd.to_numeric(series_map[resolved], errors="coerce")
            if resolved in frame.columns:
                return pd.to_numeric(frame[resolved], errors="coerce")
            raise KeyError(f"Unknown field or series reference: {resolved}")
        return pd.Series([float(resolved)] * len(frame), index=frame.index, dtype="float64")

    def _resolve_value(self, value: Any, params: dict[str, Any]) -> Any:
        if isinstance(value, str) and value.startswith("$params."):
            key = value[len("$params.") :]
            return params[key]
        return value

    def _apply_operator(self, left: float, operator: str, right: float) -> bool:
        if operator == ">":
            return left > right
        if operator == ">=":
            return left >= right
        if operator == "<":
            return left < right
        if operator == "<=":
            return left <= right
        if operator == "==":
            return left == right
        if operator == "!=":
            return left != right
        raise ValueError(f"Unsupported compare operator: {operator}")

    def _merge_matches(
        self,
        left: RuleMatch,
        right: RuleMatch,
        end_idx: int,
    ) -> RuleMatch | None:
        merged_days = dict(left.event_days)
        for event_id, day in right.event_days.items():
            existing = merged_days.get(event_id)
            if existing is not None and existing != day:
                return None
            merged_days[event_id] = day
        return RuleMatch(
            start_idx=min(left.start_idx, right.start_idx),
            end_idx=end_idx,
            event_days=merged_days,
        )

    def _dedupe_matches(self, matches: list[RuleMatch]) -> list[RuleMatch]:
        seen: set[tuple[int, int, tuple[tuple[str, int], ...]]] = set()
        output: list[RuleMatch] = []
        for match in matches:
            key = (match.start_idx, match.end_idx, tuple(sorted(match.event_days.items())))
            if key in seen:
                continue
            seen.add(key)
            output.append(match)
        return output
