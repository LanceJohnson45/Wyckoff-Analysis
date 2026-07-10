"""Compare CN legacy and mainline funnel outputs side by side."""

from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.strategy_compare import (
    StrategyRun,
    compare_strategy_runs,
    extract_l4_candidates,
)
from core.wyckoff_events import classify_wyckoff_event
from scripts.wyckoff_funnel import run_funnel_job
from utils.trading_clock import CN_TZ


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


@contextmanager
def _temp_env(pairs: dict[str, str]):
    old = {key: os.environ.get(key) for key in pairs}
    try:
        for key, value in pairs.items():
            os.environ[key] = value
        yield
    finally:
        for key, old_value in old.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _build_result(metrics: dict, triggers: dict[str, list[tuple[str, float]]]) -> SimpleNamespace:
    debug = metrics.get("_debug", {}) or {}
    return SimpleNamespace(
        triggers=triggers,
        stage_map={**(metrics.get("accum_stage_map", {}) or {}), **{str(code): "Markup" for code in (metrics.get("markup_symbols", []) or [])}},
        channel_map=dict(metrics.get("layer2_channel_map", {}) or {}),
        layer1_symbols=list(debug.get("layer1_symbols", []) or []),
        layer2_symbols=list(debug.get("layer2_symbols", []) or []),
        layer3_symbols=list(debug.get("layer3_symbols_raw", []) or []),
    )


def _slim_metrics(metrics: dict) -> dict:
    keep_keys = {
        "market",
        "end_trade_date",
        "integrity_pass",
        "integrity_fail",
        "layer1",
        "layer2",
        "layer3",
        "total_hits",
        "layer1_rejection_top",
        "layer2_rejection_top",
        "top_sectors",
        "benchmark_context",
        "quality_summary",
        "by_trigger",
    }
    return {key: metrics.get(key) for key in keep_keys if key in metrics}


def _run_engine(engine: str) -> tuple[dict[str, list[tuple[str, float]]], dict]:
    with _temp_env({"FUNNEL_MARKET": "cn", "FUNNEL_CN_ENGINE": engine}):
        return run_funnel_job(include_debug_context=True)


def _format_markdown(
    *,
    comparison,
    runs: tuple[StrategyRun, StrategyRun],
    metrics_map: dict[str, dict],
) -> str:
    lines = ["# 🔬 Wyckoff CN 双引擎影子对比", ""]
    for run in runs:
        metrics = metrics_map[run.strategy_id]
        quality = metrics.get("quality_summary") or {}
        bench = metrics.get("benchmark_context") or {}
        lines.extend(
            [
                f"## {run.strategy_id}",
                f"- L1={int(metrics.get('layer1', 0) or 0)} ｜ L2={int(metrics.get('layer2', 0) or 0)} ｜ L3={int(metrics.get('layer3', 0) or 0)} ｜ L4={int(metrics.get('total_hits', 0) or 0)}",
                f"- Regime={bench.get('regime', 'UNKNOWN')} ｜ 收盘={bench.get('close')} ｜ 广度={(bench.get('breadth') or {}).get('ratio_pct')}",
                f"- K线质量: ok={int(quality.get('ok', 0) or 0)} ｜ errors={int(quality.get('error_symbols', 0) or 0)} ｜ warnings={int(quality.get('warning_symbols', 0) or 0)}",
                "",
            ]
        )
        if run.candidates:
            lines.append("### 命中候选")
            for candidate in run.candidates[:20]:
                event = classify_wyckoff_event(
                    candidate.triggers,
                    stage=candidate.stage,
                    channel=candidate.channel,
                    score=candidate.score,
                    regime=str(bench.get("regime", "") or ""),
                )
                lines.append(
                    f"- {candidate.code} ｜ score={candidate.score:.2f} ｜ triggers={'+'.join(candidate.triggers)} ｜ event={event.label}"
                )
        else:
            lines.append("### 命中候选")
            lines.append("- 无")
        lines.append("")

    lines.extend(
        [
            "## 交集概览",
            f"- 交集={comparison.counts.get('intersection', 0)} ｜ 并集={comparison.counts.get('union', 0)}",
            f"- 仅 legacy={comparison.counts.get('only_cn_legacy', 0)} ｜ 仅 mainline={comparison.counts.get('only_cn_mainline', 0)}",
            "",
            "### 仅 legacy",
            *([f"- {code}" for code in comparison.only_by_strategy.get("cn_legacy", ())] or ["- 无"]),
            "",
            "### 仅 mainline",
            *([f"- {code}" for code in comparison.only_by_strategy.get("cn_mainline", ())] or ["- 无"]),
            "",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare CN legacy and mainline funnel outputs")
    parser.add_argument("--output-dir", default="data/strategy_shadow", help="artifact output dir")
    args = parser.parse_args(argv)

    legacy_triggers, legacy_metrics = _run_engine("legacy")
    mainline_triggers, mainline_metrics = _run_engine("mainline")

    legacy_run = StrategyRun(
        strategy_id="cn_legacy",
        result=_build_result(legacy_metrics, legacy_triggers),
        candidates=extract_l4_candidates(_build_result(legacy_metrics, legacy_triggers), "cn_legacy"),
    )
    mainline_run = StrategyRun(
        strategy_id="cn_mainline",
        result=_build_result(mainline_metrics, mainline_triggers),
        candidates=extract_l4_candidates(_build_result(mainline_metrics, mainline_triggers), "cn_mainline"),
    )
    comparison = compare_strategy_runs((legacy_run, mainline_run))

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(CN_TZ).strftime("%Y%m%d_%H%M%S")
    md_path = out_dir / f"wyckoff_cn_shadow_compare_{ts}.md"
    json_path = out_dir / f"wyckoff_cn_shadow_compare_{ts}.json"

    metrics_map = {
        "cn_legacy": _slim_metrics(legacy_metrics),
        "cn_mainline": _slim_metrics(mainline_metrics),
    }
    report = _format_markdown(comparison=comparison, runs=(legacy_run, mainline_run), metrics_map=metrics_map)
    md_path.write_text(report, encoding="utf-8")

    payload = {
        "generated_at": datetime.now(CN_TZ).isoformat(),
        "comparison": asdict(comparison),
        "runs": {
            "cn_legacy": [asdict(candidate) for candidate in legacy_run.candidates],
            "cn_mainline": [asdict(candidate) for candidate in mainline_run.candidates],
        },
        "metrics": metrics_map,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")

    print(f"[shadow] markdown: {md_path}")
    print(f"[shadow] json: {json_path}")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
