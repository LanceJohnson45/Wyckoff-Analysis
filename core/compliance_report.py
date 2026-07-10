"""Compliance-safe market brief generation for Step3."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from integrations.llm_client import OPENAI_COMPATIBLE_BASE_URLS, call_llm

OPENROUTER_PROVIDER = "openrouter"
EFFICIENCY_PROVIDER = "efficiency"
OPENROUTER_BASE_URL = OPENAI_COMPATIBLE_BASE_URLS.get("openrouter", "https://openrouter.ai/api/v1")
DEFAULT_MAX_OUTPUT_TOKENS = 2048

_STOCK_CODE_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")
_PROHIBITED_TERMS = (
    "买入",
    "卖出",
    "建仓",
    "加仓",
    "清仓",
    "减仓",
    "止损",
    "目标价",
    "参考价",
    "强烈推荐",
    "重点推荐",
    "明日买",
    "可操作",
    "PROBE",
    "ATTACK",
    "EXIT",
    "TRIM",
)


@dataclass(frozen=True)
class ComplianceLLMConfig:
    provider: str
    api_key: str
    model: str
    base_url: str
    source: str


@dataclass(frozen=True)
class ComplianceValidation:
    ok: bool
    reasons: tuple[str, ...] = ()


def _bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default


def fmt_pct(value: Any) -> str:
    num = pd.to_numeric(value, errors="coerce")
    if pd.isna(num):
        return "待更新"
    x = float(num)
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.2f}%"


def resolve_compliance_llm_config() -> ComplianceLLMConfig | None:
    openrouter_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    openrouter_model = os.getenv("OPENROUTER_MODEL", "").strip()
    if openrouter_key and openrouter_model:
        return ComplianceLLMConfig(
            provider=OPENROUTER_PROVIDER,
            api_key=openrouter_key,
            model=openrouter_model,
            base_url=OPENROUTER_BASE_URL,
            source="openrouter",
        )
    efficiency_key = os.getenv("EFFICIENCY_API_KEY", "").strip()
    efficiency_model = os.getenv("EFFICIENCY_MODEL", "").strip()
    efficiency_base_url = os.getenv("EFFICIENCY_BASE_URL", "").strip()
    if efficiency_key and efficiency_model and efficiency_base_url:
        return ComplianceLLMConfig(
            provider=EFFICIENCY_PROVIDER,
            api_key=efficiency_key,
            model=efficiency_model,
            base_url=efficiency_base_url,
            source="efficiency",
        )
    return None


def _score_bucket(score: float) -> str:
    if score >= 0.75:
        return "高"
    if score >= 0.45:
        return "中"
    return "低"


def build_public_payload(
    *,
    benchmark_context: dict,
    selected_df: pd.DataFrame,
    ops_codes: list[str] | None = None,
    rag_veto_count: int = 0,
) -> dict[str, Any]:
    ctx = benchmark_context or {}
    breadth = ctx.get("breadth", {}) or {}
    df = selected_df.copy() if isinstance(selected_df, pd.DataFrame) else pd.DataFrame()
    ops_set = {str(code).strip() for code in (ops_codes or []) if str(code).strip()}
    payload: dict[str, Any] = {
        "trade_date": str(ctx.get("trade_date") or ctx.get("end_trade_date") or ""),
        "market": {
            "regime": str(ctx.get("regime", "NEUTRAL") or "NEUTRAL").strip().upper(),
            "main_today_pct": fmt_pct(ctx.get("main_today_pct")),
            "recent3_cum_pct": fmt_pct(ctx.get("recent3_cum_pct")),
            "breadth_ratio": fmt_pct(breadth.get("ratio_pct")),
            "volume_state": str(ctx.get("main_volume_state", "") or "").strip() or "待更新",
        },
        "sample_stats": {
            "candidate_count": int(len(df)),
            "springboard_count": int(len(ops_set)),
            "rag_veto_count": int(max(rag_veto_count, 0)),
        },
        "style_stats": {},
        "sector_stats": [],
        "risk_flags": [],
    }
    if not df.empty:
        track_series = df.get("track", pd.Series(dtype=str)).astype(str).str.strip()
        payload["style_stats"] = {
            "trend_count": int((track_series == "Trend").sum()),
            "accum_count": int((track_series == "Accum").sum()),
            "unknown_count": int((~track_series.isin(["Trend", "Accum"])).sum()),
        }
        tag_series = df.get("tag", pd.Series(dtype=str)).astype(str).str.lower()
        payload["trigger_stats"] = {
            "sos_count": int(tag_series.str.contains("sos|点火|突破", regex=True).sum()),
            "spring_count": int(tag_series.str.contains("spring", regex=True).sum()),
            "lps_count": int(tag_series.str.contains("lps", regex=True).sum()),
            "evr_count": int(tag_series.str.contains("evr", regex=True).sum()),
        }
        sec_df = df.copy()
        if "industry" in sec_df.columns:
            sec_df["industry"] = sec_df["industry"].astype(str).str.strip()
        else:
            sec_df["industry"] = ""
        sec_df = sec_df[sec_df["industry"] != ""]
        if not sec_df.empty:
            priority_raw = sec_df["priority_score"] if "priority_score" in sec_df.columns else pd.Series([pd.NA] * len(sec_df), index=sec_df.index)
            funnel_raw = sec_df["funnel_score"] if "funnel_score" in sec_df.columns else pd.Series([pd.NA] * len(sec_df), index=sec_df.index)
            score_series = pd.to_numeric(priority_raw, errors="coerce")
            score_series = score_series.where(score_series.notna(), pd.to_numeric(funnel_raw, errors="coerce"))
            sec_df["score"] = score_series.fillna(0.0)
            grouped = (
                sec_df.groupby("industry", as_index=False)
                .agg(sample_count=("industry", "count"), avg_score=("score", "mean"))
                .sort_values(["sample_count", "avg_score"], ascending=[False, False])
                .head(5)
            )
            payload["sector_stats"] = [
                {
                    "industry": str(row["industry"]),
                    "sample_count": int(row["sample_count"]),
                    "score_bucket": _score_bucket(float(row["avg_score"])),
                }
                for _, row in grouped.iterrows()
            ]
    regime = payload["market"]["regime"]
    if regime in {"RISK_OFF", "CRASH", "BLACK_SWAN"}:
        payload["risk_flags"].append("市场风险偏高，弱市假突破与流动性折价需要重点防范")
    if payload["sample_stats"]["rag_veto_count"] > 0:
        payload["risk_flags"].append("本轮存在负面信息防雷剔除样本")
    if payload["sample_stats"]["candidate_count"] <= 0:
        payload["risk_flags"].append("本轮可观察样本不足")
    if not payload["risk_flags"]:
        payload["risk_flags"].append("当前观察以结构跟踪为主，请结合后续量价确认")
    return payload


def validate_compliance_report(text: str, *, forbidden_names: list[str] | tuple[str, ...] | None = None) -> ComplianceValidation:
    raw = str(text or "")
    reasons: list[str] = []
    if _STOCK_CODE_RE.search(raw):
        reasons.append("contains_stock_code")
    for name in forbidden_names or ():
        name_s = str(name or "").strip()
        if name_s and name_s in raw:
            reasons.append("contains_stock_name")
            break
    for term in _PROHIBITED_TERMS:
        if term and term in raw:
            reasons.append(f"contains_term:{term}")
    return ComplianceValidation(ok=not reasons, reasons=tuple(reasons))


def _fallback_brief(payload: dict[str, Any]) -> str:
    market = payload.get("market", {}) or {}
    sectors = payload.get("sector_stats", []) or []
    risks = payload.get("risk_flags", []) or []
    sample_stats = payload.get("sample_stats", {}) or {}
    lines = [
        "## 今日市场观察简报（合规版）",
        "",
        "⚠️ 以下内容仅用于市场研究交流，不构成任何投资建议。",
        "",
        "### 一、市场状态",
        (
            f"- 制度: {market.get('regime', 'NEUTRAL')} | 当日涨跌: {market.get('main_today_pct', '待更新')} | "
            f"近3日累计: {market.get('recent3_cum_pct', '待更新')} | 广度: {market.get('breadth_ratio', '待更新')} | "
            f"量能: {market.get('volume_state', '待更新')}"
        ),
        f"- 候选样本数: {int(sample_stats.get('candidate_count', 0) or 0)}",
        "",
        "### 二、板块观察",
    ]
    if sectors:
        for item in sectors[:5]:
            lines.append(f"- {item.get('industry', '未知行业')}（样本{int(item.get('sample_count', 0) or 0)}，强度{item.get('score_bucket', '低')}）")
    else:
        lines.append("- 暂无明显板块聚集特征")
    lines.extend(["", "### 三、风险提示"])
    for risk in risks:
        lines.append(f"- {risk}")
    lines.extend(
        [
            "",
            "### 合规声明",
            "- 本简报仅用于市场研究与信息交流，不构成个股推荐或买卖建议。",
            "- 不承诺收益，不涉及代客理财或代客下单。",
            "- 股市有风险，投资需谨慎。",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def _prompt_from_payload(payload: dict[str, Any]) -> tuple[str, str]:
    system = (
        "你是中文市场观察简报编辑。请根据提供的去标识化市场数据，"
        "生成一份对外可发的合规摘要。"
        "绝对不要出现个股代码、个股名称、买卖建议、仓位建议、目标价、止损价。"
        "只允许讨论市场状态、板块聚集、风险提示和研究观察。"
    )
    user = (
        "请将以下 JSON 数据整理成 3 段以内的中文 Markdown 简报，"
        "保留'市场状态 / 板块观察 / 风险提示 / 合规声明'这类结构。\n\n"
        f"{payload}"
    )
    return system, user


def generate_compliance_brief(
    *,
    benchmark_context: dict,
    selected_df: pd.DataFrame,
    ops_codes: list[str] | None = None,
    code_name: dict[str, str] | None = None,
    rag_veto_count: int = 0,
) -> str:
    payload = build_public_payload(
        benchmark_context=benchmark_context,
        selected_df=selected_df,
        ops_codes=ops_codes,
        rag_veto_count=rag_veto_count,
    )
    fallback = _fallback_brief(payload)
    if not _bool_env("STEP3_COMPLIANCE_USE_LLM", True):
        return fallback
    cfg = resolve_compliance_llm_config()
    if cfg is None:
        return fallback
    forbidden_names = [str(v).strip() for v in (code_name or {}).values() if str(v).strip()]
    retries = max(_int_env("STEP3_COMPLIANCE_MAX_RETRIES", 1), 0)
    system_prompt, user_message = _prompt_from_payload(payload)
    for _ in range(retries + 1):
        try:
            text = call_llm(
                provider=("openai" if cfg.provider in {OPENROUTER_PROVIDER, EFFICIENCY_PROVIDER} else cfg.provider),
                model=cfg.model,
                api_key=cfg.api_key,
                system_prompt=system_prompt,
                user_message=user_message,
                base_url=cfg.base_url or None,
                timeout=180,
                max_output_tokens=DEFAULT_MAX_OUTPUT_TOKENS,
            )
        except Exception:
            continue
        validation = validate_compliance_report(text, forbidden_names=forbidden_names)
        if validation.ok:
            return str(text).strip() + "\n"
    return fallback


__all__ = [
    "ComplianceLLMConfig",
    "ComplianceValidation",
    "OPENROUTER_BASE_URL",
    "build_public_payload",
    "generate_compliance_brief",
    "resolve_compliance_llm_config",
    "validate_compliance_report",
]
