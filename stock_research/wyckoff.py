"""固定快照驱动美股生产漏斗，不复制或修改信号规则。"""

import hashlib
import math
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Literal, get_type_hints

import numpy as np
import pandas as pd
from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from core.market_funnel_config import apply_funnel_config_overrides, funnel_config_for_market
from core.wyckoff_engine import FunnelConfig
from stock_research.identity import canonical_json, digest, now
from stock_research.models import FrozenModel, Snapshot
from workflows.market_funnel_job import _candidate_rows, _cap_candidates_for_regime, _run_layers
from workflows.market_funnel_runtime import runtime_config_from_env


class WyckoffParameters(FrozenModel):
    """生产默认值加显式研究覆盖；不接受未知字段或隐式环境覆盖。"""

    version: str = "baseline-us-v1"
    algorithm: Literal["production-us-l1-l4-v1"] = "production-us-l1-l4-v1"
    pool_size: int = Field(default=30, ge=1, le=100)
    overrides: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_overrides(self) -> "WyckoffParameters":
        """复用上游字段校验，并按真实数据类型拒绝字符串布尔等错误。"""
        cfg = funnel_config_for_market("us")
        hints = get_type_hints(FunnelConfig)
        if self.overrides.keys() - hints.keys():
            raise ValueError("未知 FunnelConfig 覆盖字段")
        for key, value in self.overrides.items():
            TypeAdapter(hints[key]).validate_json(canonical_json(value), strict=True)
        apply_funnel_config_overrides(cfg, self.overrides)
        if cfg.min_avg_amount_wan != 0:
            raise ValueError("快照缺少可核验成交金额，不支持非零金额硬门槛")
        if cfg.require_cn_main_or_chinext or cfg.min_market_cap_yi != 0:
            raise ValueError("美股研究不支持A股板块或缺失市值的硬门槛")
        if cfg.trading_days != 320:
            raise ValueError("当前Wyckoff固定320根窗口，不支持修改trading_days")
        return self


class WyckoffReport(FrozenModel):
    """生产量化事实与观察池，完整保留上游诊断但不生成执行许可。"""

    schema_version: Literal[1] = 1
    snapshot_id: str
    snapshot_hash: str
    as_of: date
    parameters: WyckoffParameters
    parameter_hash: str
    engine_hash: str
    create_time: AwareDatetime
    effective_config: dict[str, object]
    metrics: dict[str, object]
    observations: tuple[dict[str, object], ...]
    candidates: tuple[dict[str, object], ...]
    excluded: tuple[dict[str, object], ...]
    auxiliary: dict[str, object]
    pool: tuple[str, ...]
    model_selection: None = None
    execution_permission: Literal["not_evaluated"] = "not_evaluated"
    limitations: tuple[str, ...]
    content_hash: str

    @model_validator(mode="after")
    def verify_content(self) -> "WyckoffReport":
        """独立重读校验完整摘要和池来源，不能接收损坏缓存。"""
        body = self.model_dump(mode="json", exclude={"content_hash"})
        if digest(body) != self.content_hash or self.parameter_hash != digest(self.parameters.model_dump(mode="json")):
            raise ValueError("Wyckoff观察产物内容或参数摘要不匹配")
        if len(set(self.pool)) != len(self.pool) or self.pool != tuple(
            row["symbol"] for row in self.candidates[: self.parameters.pool_size]
        ):
            raise ValueError("Wyckoff观察池与生产排序不匹配")
        return self


def engine_hash() -> str:
    """源码改变必须使缓存失效，避免沿用旧配置或间接依赖的结论。"""
    root = Path(__file__).resolve().parent.parent
    paths = sorted(
        {
            Path(__file__).resolve(),
            *root.glob("core/**/*.py"),
            *root.glob("workflows/market_funnel*.py"),
            root / "workflows/step3_entry_quality.py",
        }
    )
    return digest(
        {
            "sources": {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
            "numpy": np.__version__,
            "pandas": pd.__version__,
        }
    )


def json_values(value: object) -> object:
    """上游NumPy标量转标准JSON，非有限辅助值明确标缺失。"""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, dict):
        return {key: json_values(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_values(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def production_inputs(snapshot: Snapshot) -> tuple[list[str], dict[str, pd.DataFrame], pd.DataFrame, dict, dict]:
    """日期仅来自已校验快照；不传amount以保留上游代理的真实使用事实。"""
    grouped = defaultdict(list)
    eligible = set(snapshot.quality.eligible["wyckoff"])
    for bar in snapshot.bars:
        if (bar.kind == "stock" and bar.symbol in eligible) or (bar.kind == "benchmark" and bar.symbol == "SPY"):
            grouped[bar.kind, bar.symbol].append(
                {
                    "date": bar.session_date.isoformat(),
                    **{key: getattr(bar, key) for key in ("open", "high", "low", "close", "volume")},
                }
            )
    frames = {}
    for key, rows in grouped.items():
        frame = pd.DataFrame(rows).sort_values("date").tail(320).reset_index(drop=True)
        frame["pct_chg"] = frame["close"].pct_change(fill_method=None) * 100
        frames[key] = frame
    names = {item.symbol: item.name for item in snapshot.universe.members if item.effective_date <= snapshot.as_of}
    sectors = {
        item.symbol: item.industry
        for item in snapshot.classifications.records
        if item.industry and item.effective_date <= snapshot.as_of
    }
    return (
        sorted(eligible),
        {symbol: frames["stock", symbol] for symbol in eligible},
        frames["benchmark", "SPY"],
        names,
        sectors,
    )


def production_result(snapshot: Snapshot, parameters: WyckoffParameters) -> tuple[dict, list[dict], dict]:
    """与生产run_funnel_for_ranked共用层计算、候选富化及水温限制。"""
    symbols, frames, benchmark, names, sectors = production_inputs(snapshot)
    runtime = runtime_config_from_env("us", None)
    # 网络/报价字段不进入离线计算；冻结全部真正被_run_layers读取的运行配置。
    runtime = replace(runtime, kline_count=320, min_avg_amount=0.0)
    diagnostics = {}
    triggers, metrics = _run_layers(
        symbols,
        names,
        frames,
        runtime,
        bench_df=benchmark,
        sector_map=sectors,
        cfg_overrides=parameters.overrides,
        diagnostics=diagnostics,
    )
    raw = _candidate_rows(triggers, name_map=names, df_map=frames, bench_df=benchmark, market="us")
    candidates = _cap_candidates_for_regime(raw, metrics)
    metrics.update(benchmark_symbol="SPY", actionable_count=sum(bool(row.get("actionable")) for row in candidates))
    diagnostics["triggers"] = triggers
    diagnostics["regime_excluded"] = [row["symbol"] for row in raw if row not in candidates]
    return metrics, candidates, diagnostics


def compute_wyckoff(snapshot: Snapshot, parameters: WyckoffParameters) -> WyckoffReport:
    """只将正式L4命中经水温限制后的原排序传入复核池。"""
    if not snapshot.quality.passed:
        raise ValueError("数据质量未通过，不生成Wyckoff量化池")
    metrics, candidates, trace = production_result(snapshot, parameters)
    eligible = snapshot.quality.eligible["wyckoff"]
    hits = {row["symbol"] for row in candidates}
    triggered = {symbol for rows in trace["triggers"].values() for symbol, _score in rows}
    observations, excluded = (
        [],
        [issue.model_dump(mode="json") for issue in snapshot.quality.issues if issue.strategy in {"all", "wyckoff"}],
    )
    for symbol in eligible:
        passed = {name: symbol in symbols for name, symbols in trace["layers"].items()}
        observations.append({"symbol": symbol, **passed, "layer4": symbol in triggered, "qualified": symbol in hits})
        if symbol not in hits:
            layer = next(
                (name for name, value in passed.items() if not value), "layer4" if symbol not in triggered else "regime"
            )
            reason = trace["layer2_rejections"].get(symbol) if layer == "layer2" else None
            excluded.append({"symbol": symbol, "layer": layer, "reason": reason or f"production_{layer}_not_passed"})
    body = json_values(
        dict(
            schema_version=1,
            snapshot_id=snapshot.id,
            snapshot_hash=snapshot.content_hash,
            as_of=snapshot.as_of,
            parameters=parameters.model_dump(mode="json"),
            parameter_hash=digest(parameters.model_dump(mode="json")),
            engine_hash=engine_hash(),
            create_time=now(),
            effective_config=trace["effective_config"],
            metrics=metrics,
            observations=observations,
            candidates=candidates,
            excluded=excluded,
            auxiliary={
                "pre_ignition": trace["pre_ignition"],
                "trend_watch": metrics["trend_watch_rows"],
                "regime_excluded": trace["regime_excluded"],
                "triggers": trace["triggers"],
            },
            pool=[row["symbol"] for row in candidates[: parameters.pool_size]],
            model_selection=None,
            execution_permission="not_evaluated",
            limitations=(
                *snapshot.quality.limitations,
                "复用美股生产L1-L4及水温限制；未执行TickFlow实时报价预筛，股票池为固定研究池",
                "快照缺少可核验成交金额：金额硬门槛固定为0；上游辅助入场评分使用close×volume代理，口径未确认",
                "candidates/metrics中的actionable等字段为上游原始诊断；不代表研究执行许可、模型初选或BUY",
                "市值、实时新闻及财务数据缺失；未推断主题或调用旁路补位，未验证费用后绩效",
            ),
        )
    )
    return WyckoffReport(**body, content_hash=digest(body))
