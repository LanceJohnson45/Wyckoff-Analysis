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
    SymbolEvaluationContext,
    infer_data_requirements,
    load_indicator_definition,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_QUERY_DIR = PROJECT_ROOT / "query"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "analysis_results"
_EXCLUDED_SPEC_FILES = {"extraction_progress.json"}
MAX_DAILY_BARS = 240


@dataclass(frozen=True)
class ThreeHundredDayIndicatorHit:
    indicator_index: int
    indicator: str
    signal_bias: str
    source_pages: tuple[int, ...]
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
    index: int
    file_name: str
    definition: dict[str, Any]
    min_bars: int
    required_fields: tuple[str, ...]


HistoryLoader = Callable[[str, int], pd.DataFrame | None]
IndicatorProgressCallback = Callable[[dict[str, Any]], None]


@lru_cache(maxsize=4)
def _load_computable_specs(query_dir: str) -> tuple[_LoadedSpec, ...]:
    specs: list[_LoadedSpec] = []
    spec_index = 0
    for path in sorted(Path(query_dir).glob("*.json")):
        if path.name in _EXCLUDED_SPEC_FILES:
            continue
        definition = load_indicator_definition(path)
        if not definition.get("computable_from_daily_ocvhl", False):
            continue
        requirements = infer_data_requirements(definition)
        if requirements.min_bars > MAX_DAILY_BARS:
            continue
        spec_index += 1
        specs.append(
            _LoadedSpec(
                index=spec_index,
                file_name=path.name,
                definition=definition,
                min_bars=requirements.min_bars,
                required_fields=requirements.required_fields,
            )
        )
    return tuple(specs)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_signal_bias(value: Any) -> str:
    raw = _clean_text(value).lower()
    alias_map = {
        "bullish": "bullish",
        "long": "bullish",
        "buy": "bullish",
        "看多": "bullish",
        "多头": "bullish",
        "bearish": "bearish",
        "short": "bearish",
        "sell": "bearish",
        "看空": "bearish",
        "空头": "bearish",
        "neutral": "neutral",
        "observe": "neutral",
        "观察": "neutral",
        "中性": "neutral",
    }
    return alias_map.get(raw, "")


def _infer_signal_bias(definition: dict[str, Any], file_name: str) -> str:
    explicit = _normalize_signal_bias(definition.get("signal_bias"))
    if explicit:
        return explicit

    text = " ".join(
        [
            str(definition.get("name", "")),
            str(definition.get("source_summary", "")),
            str(definition.get("comment", "")),
            str(definition.get("explain", "")),
            str(file_name),
        ]
    ).lower()

    bearish_keywords = (
        "见顶",
        "顶部",
        "头部",
        "卖",
        "卖点",
        "空头",
        "看空",
        "风险",
        "墓碑",
        "断头",
        "死叉",
        "下穿",
        "跌破",
        "下跌",
        "回落",
        "出货",
        "崩",
        "杀跌",
        "不妙",
        "压力",
        "天灵盖",
        "guillotine",
        "bearish",
        "death",
        "top",
        "crash",
        "risk",
        "exit",
        "skullcap",
    )
    bullish_keywords = (
        "买",
        "买点",
        "多头",
        "看多",
        "启动",
        "起涨",
        "起飞",
        "突破",
        "上穿",
        "拉升",
        "黑马",
        "支撑",
        "生命线",
        "反弹",
        "低吸",
        "进场",
        "持有",
        "加速爬升",
        "创新高",
        "走强",
        "bullish",
        "golden",
        "breakout",
        "support",
        "lifeline",
        "rally",
        "surge",
        "entry",
        "bottom",
        "bounce",
    )
    neutral_keywords = (
        "方法",
        "方法论",
        "keypoints",
        "methodology",
        "selection",
        "operating plan",
        "plan",
        "理论",
        "规则",
    )

    if any(keyword in text for keyword in bearish_keywords):
        return "bearish"
    if any(keyword in text for keyword in bullish_keywords):
        return "bullish"
    if any(keyword in text for keyword in neutral_keywords):
        return "neutral"
    return "neutral"


def _signal_bias_label(signal_bias: str) -> str:
    return {
        "bullish": "多头",
        "bearish": "空头",
        "neutral": "中性",
    }.get(signal_bias, "中性")


def _normalize_source_pages(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list):
        return tuple()
    pages: list[int] = []
    for item in value:
        try:
            page = int(item)
        except (TypeError, ValueError):
            continue
        if page > 0:
            pages.append(page)
    return tuple(pages)


def _format_source_pages(source_pages: tuple[int, ...]) -> str:
    if not source_pages:
        return "页码未知"
    if len(source_pages) == 1:
        return f"第{source_pages[0]}页"
    return "第" + "、".join(str(page) for page in source_pages) + "页"


def _format_indicator_index(indicator_index: int) -> str:
    return f"{indicator_index:03d}"


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
            f"[{_format_indicator_index(hit.indicator_index)}][{hit.spec_file}][{_signal_bias_label(hit.signal_bias)}]{hit.indicator}（{_format_source_pages(hit.source_pages)}） 原因：{hit.reason}"
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
    progress_callback: IndicatorProgressCallback | None = None,
) -> list[ThreeHundredDayIndicatorHit]:
    indicators: list[ThreeHundredDayIndicatorHit] = []
    loaded_frame = frame
    loaded_bars = len(frame) if frame is not None and not frame.empty else 0
    symbol_context: SymbolEvaluationContext | None = None

    if history_loader is not None and loaded_bars <= 0:
        max_required_bars = max((spec.min_bars for spec in specs), default=0)
        if max_required_bars > 0:
            fetched = history_loader(code, max_required_bars)
            if fetched is not None and not fetched.empty:
                loaded_frame = fetched.reset_index(drop=True)
                loaded_bars = len(loaded_frame)

    if loaded_frame is not None and not loaded_frame.empty:
        symbol_context = engine.build_symbol_context(loaded_frame)

    for spec in specs:
        signal_bias = _infer_signal_bias(spec.definition, spec.file_name)
        source_pages = _normalize_source_pages(spec.definition.get("source_pages"))
        if loaded_frame is None or loaded_frame.empty:
            if progress_callback is not None:
                progress_callback(
                    {
                        "code": code,
                        "name": name,
                        "indicator_index": spec.index,
                        "indicator": str(spec.definition.get("name", spec.file_name)),
                        "signal_bias": signal_bias,
                        "source_pages": list(source_pages),
                        "spec_file": spec.file_name,
                        "required_bars": spec.min_bars,
                        "loaded_bars": loaded_bars,
                        "matched": False,
                        "status": "no_data",
                        "signal_date": None,
                        "reason": "无可用日线数据。",
                    }
                )
            continue
        if loaded_bars < spec.min_bars:
            if progress_callback is not None:
                progress_callback(
                    {
                        "code": code,
                        "name": name,
                        "indicator_index": spec.index,
                        "indicator": str(spec.definition.get("name", spec.file_name)),
                        "signal_bias": signal_bias,
                        "source_pages": list(source_pages),
                        "spec_file": spec.file_name,
                        "required_bars": spec.min_bars,
                        "loaded_bars": loaded_bars,
                        "matched": False,
                        "status": "insufficient_bars",
                        "signal_date": None,
                        "reason": f"可用日线不足，要求 {spec.min_bars} 根，实际 {loaded_bars} 根。",
                    }
                )
            continue

        frame_columns = set(loaded_frame.columns)
        if not set(spec.required_fields).issubset(frame_columns):
            missing_fields = sorted(set(spec.required_fields) - frame_columns)
            if progress_callback is not None:
                progress_callback(
                    {
                        "code": code,
                        "name": name,
                        "indicator_index": spec.index,
                        "indicator": str(spec.definition.get("name", spec.file_name)),
                        "signal_bias": signal_bias,
                        "source_pages": list(source_pages),
                        "spec_file": spec.file_name,
                        "required_bars": spec.min_bars,
                        "loaded_bars": loaded_bars,
                        "matched": False,
                        "status": "missing_fields",
                        "signal_date": None,
                        "reason": f"缺少字段: {', '.join(missing_fields)}",
                    }
                )
            continue
        evaluation = engine.evaluate(
            loaded_frame,
            spec.definition,
            context=symbol_context,
        )
        signal_date = (
            str(evaluation.signal_date)
            if evaluation.signal_date not in {None, ""}
            else None
        )
        matched = bool(evaluation.latest_match)
        if progress_callback is not None:
            progress_callback(
                {
                    "code": code,
                    "name": name,
                    "indicator_index": spec.index,
                    "indicator": str(spec.definition.get("name", spec.file_name)),
                    "signal_bias": signal_bias,
                    "source_pages": list(source_pages),
                    "spec_file": spec.file_name,
                    "required_bars": spec.min_bars,
                    "loaded_bars": loaded_bars,
                    "matched": matched,
                    "status": "evaluated",
                    "signal_date": signal_date,
                    "reason": _build_reason(spec.definition, signal_date)
                    if matched
                    else "指标未命中。",
                }
            )
        if not matched:
            continue
        indicators.append(
            ThreeHundredDayIndicatorHit(
                indicator_index=spec.index,
                indicator=str(spec.definition.get("name", spec.file_name)),
                signal_bias=signal_bias,
                source_pages=source_pages,
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
    progress_callback: IndicatorProgressCallback | None = None,
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
            progress_callback=progress_callback,
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
