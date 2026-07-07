# -*- coding: utf-8 -*-
"""
300day 日线指标批量扫描引擎。

职责：
- 加载 query/ 下可由日线 O/H/L/C/V 计算的指标定义
- 对 A 股股票批量执行 latest_match 判断
- 将命中结果写入单独的当日结果文件
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from functools import lru_cache
import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from core.indicator_rule_engine import (
    IndicatorRuleEngine,
    infer_data_requirements,
    load_indicator_definition,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_QUERY_DIR = PROJECT_ROOT / "query"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "analysis_results"
_EXCLUDED_SPEC_FILES = {"extraction_progress.json"}


@dataclass(frozen=True)
class ThreeHundredDayIndicatorHit:
    indicator: str
    reason: str
    signal_date: str | None
    spec_file: str


@dataclass(frozen=True)
class ThreeHundredDaySymbolHit:
    code: str
    name: str
    hit_count: int
    indicators: tuple[ThreeHundredDayIndicatorHit, ...]


@dataclass(frozen=True)
class ThreeHundredDayScanResult:
    market: str
    trade_date: str
    scanned_symbols: int
    matched_symbols: int
    matched_indicator_total: int
    output_path: str
    symbols: tuple[ThreeHundredDaySymbolHit, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _LoadedSpec:
    file_name: str
    definition: dict[str, Any]
    min_bars: int
    required_fields: tuple[str, ...]


HistoryLoader = Callable[[str, int], pd.DataFrame | None]


@lru_cache(maxsize=4)
def _load_computable_specs(query_dir: str) -> tuple[_LoadedSpec, ...]:
    specs: list[_LoadedSpec] = []
    for path in sorted(Path(query_dir).glob("*.json")):
        if path.name in _EXCLUDED_SPEC_FILES:
            continue
        definition = load_indicator_definition(path)
        if not definition.get("computable_from_daily_ocvhl", False):
            continue
        requirements = infer_data_requirements(definition)
        specs.append(
            _LoadedSpec(
                file_name=path.name,
                definition=definition,
                min_bars=requirements.min_bars,
                required_fields=requirements.required_fields,
            )
        )
    return tuple(specs)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _build_reason(definition: dict[str, Any], signal_date: Any) -> str:
    base = (
        _clean_text(definition.get("source_summary"))
        or _clean_text(definition.get("explain"))
        or _clean_text(definition.get("comment"))
        or "指标规则命中。"
    )
    note = _clean_text(definition.get("comment"))
    if note and note != base:
        base = f"{base} 注意：{note}"
    if signal_date in {None, ""}:
        return base
    return f"{base}（信号日：{signal_date}）"


def _write_text_report(
    *,
    market: str,
    trade_date: str,
    results: list[ThreeHundredDaySymbolHit],
    output_dir: Path,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    market_label = {"cn": "A股", "us": "美股", "hk": "港股"}.get(market, market.upper())
    total_hits = sum(item.hit_count for item in results)
    path = output_dir / f"{trade_date}_300day记录.txt"

    lines = [
        f"{market_label} 300day 扫描结果",
        f"交易日：{trade_date}",
        f"达标股票数：{len(results)}",
        f"达标指标总数：{total_hits}",
        "",
    ]
    for item in results:
        indicator_parts = [
            f"{hit.indicator} 原因：{hit.reason}"
            for hit in item.indicators
        ]
        lines.append(
            f"{market_label} 个股：{item.code} {item.name} 今日达标："
            + "；".join(indicator_parts)
        )

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return path


def _evaluate_symbol_hits(
    *,
    code: str,
    name: str,
    specs: tuple[_LoadedSpec, ...],
    engine: IndicatorRuleEngine,
    frame: pd.DataFrame | None = None,
    history_loader: HistoryLoader | None = None,
) -> list[ThreeHundredDayIndicatorHit]:
    indicators: list[ThreeHundredDayIndicatorHit] = []
    loaded_frame = frame
    loaded_bars = len(frame) if frame is not None and not frame.empty else 0
    unavailable_min_bars = 0

    for spec in specs:
        if history_loader is not None and spec.min_bars > loaded_bars and spec.min_bars > unavailable_min_bars:
            fetched = history_loader(code, spec.min_bars)
            if fetched is not None and not fetched.empty:
                loaded_frame = fetched.reset_index(drop=True)
                loaded_bars = len(loaded_frame)
            else:
                unavailable_min_bars = spec.min_bars

        if loaded_frame is None or loaded_frame.empty:
            continue
        if loaded_bars < spec.min_bars:
            continue

        frame_columns = set(loaded_frame.columns)
        if not set(spec.required_fields).issubset(frame_columns):
            continue
        evaluation = engine.evaluate(loaded_frame, spec.definition)
        if not evaluation.latest_match:
            continue
        signal_date = (
            str(evaluation.signal_date)
            if evaluation.signal_date not in {None, ""}
            else None
        )
        indicators.append(
            ThreeHundredDayIndicatorHit(
                indicator=str(spec.definition.get("name", spec.file_name)),
                reason=_build_reason(spec.definition, signal_date),
                signal_date=signal_date,
                spec_file=spec.file_name,
            )
        )
    return indicators


def scan_three_hundred_day(
    *,
    df_map: dict[str, pd.DataFrame],
    name_map: dict[str, str] | None,
    trade_date: str,
    market: str = "cn",
    query_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> ThreeHundredDayScanResult:
    query_path = Path(query_dir or DEFAULT_QUERY_DIR)
    output_path = Path(output_dir or DEFAULT_OUTPUT_DIR)
    specs = _load_computable_specs(str(query_path))
    engine = IndicatorRuleEngine()
    safe_name_map = name_map or {}

    symbol_hits: list[ThreeHundredDaySymbolHit] = []
    matched_indicator_total = 0

    for code in sorted(df_map.keys()):
        frame = df_map.get(code)
        if frame is None or frame.empty:
            continue
        indicators = _evaluate_symbol_hits(
            code=code,
            name=str(safe_name_map.get(code, code) or code).strip(),
            specs=specs,
            engine=engine,
            frame=frame,
            history_loader=None,
        )
        if not indicators:
            continue

        indicators = sorted(indicators, key=lambda item: item.indicator)
        matched_indicator_total += len(indicators)
        symbol_hits.append(
            ThreeHundredDaySymbolHit(
                code=str(code).strip(),
                name=str(safe_name_map.get(code, code) or code).strip(),
                hit_count=len(indicators),
                indicators=tuple(indicators),
            )
        )

    symbol_hits.sort(key=lambda item: (-item.hit_count, item.code))
    report_path = _write_text_report(
        market=market,
        trade_date=trade_date,
        results=symbol_hits,
        output_dir=output_path,
    )
    return ThreeHundredDayScanResult(
        market=market,
        trade_date=trade_date,
        scanned_symbols=len(df_map),
        matched_symbols=len(symbol_hits),
        matched_indicator_total=matched_indicator_total,
        output_path=str(report_path),
        symbols=tuple(symbol_hits),
    )


def scan_three_hundred_day_lazy(
    *,
    symbols: list[str],
    name_map: dict[str, str] | None,
    trade_date: str,
    history_loader: HistoryLoader,
    market: str = "cn",
    query_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> ThreeHundredDayScanResult:
    query_path = Path(query_dir or DEFAULT_QUERY_DIR)
    output_path = Path(output_dir or DEFAULT_OUTPUT_DIR)
    specs = tuple(sorted(_load_computable_specs(str(query_path)), key=lambda item: item.min_bars))
    engine = IndicatorRuleEngine()
    safe_name_map = name_map or {}

    symbol_hits: list[ThreeHundredDaySymbolHit] = []
    matched_indicator_total = 0

    for code in sorted(str(code).strip() for code in symbols if str(code).strip()):
        indicators = _evaluate_symbol_hits(
            code=code,
            name=str(safe_name_map.get(code, code) or code).strip(),
            specs=specs,
            engine=engine,
            frame=None,
            history_loader=history_loader,
        )
        if not indicators:
            continue
        indicators = sorted(indicators, key=lambda item: item.indicator)
        matched_indicator_total += len(indicators)
        symbol_hits.append(
            ThreeHundredDaySymbolHit(
                code=code,
                name=str(safe_name_map.get(code, code) or code).strip(),
                hit_count=len(indicators),
                indicators=tuple(indicators),
            )
        )

    symbol_hits.sort(key=lambda item: (-item.hit_count, item.code))
    report_path = _write_text_report(
        market=market,
        trade_date=trade_date,
        results=symbol_hits,
        output_dir=output_path,
    )
    return ThreeHundredDayScanResult(
        market=market,
        trade_date=trade_date,
        scanned_symbols=len({str(code).strip() for code in symbols if str(code).strip()}),
        matched_symbols=len(symbol_hits),
        matched_indicator_total=matched_indicator_total,
        output_path=str(report_path),
        symbols=tuple(symbol_hits),
    )
