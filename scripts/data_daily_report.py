# -*- coding: utf-8 -*-
"""
数据日报：汇总漏斗执行情况并推送飞书。

指标覆盖：
  - 执行时间 / 目标交易日 / 盘前盘后判断
  - 股票池总量 / 数据拉取成功 / 失败 / 完整性筛除
  - 缓存命中 vs 新拉取（需 FUNNEL_REPORT_CACHE_STATS=1 并在 stock_hist_repository 层注入计数器）
  - 漏斗各层（L0数据 → L1 → L2 → L3 → L4）通过数量及通过率
  - 大盘制度 / 当日涨跌 / 面包量指标
  - L1 / L2 拒绝 Top 原因
  - 各 L4 触发器命中数
  - 异常预警（数据失败率过高 / 任何层通过率异常低）

使用方式：
  由 daily_job.py 调用（传入 funnel metrics），或独立运行（自动执行漏斗 + 出报告）。

环境变量：
  DATA_REPORT_WEBHOOK_URL  飞书 webhook（专属日报机器人）
  FEISHU_WEBHOOK_URL       备用（若日报 webhook 未配置则使用此值）
  FUNNEL_MARKET            cn / hk / us（影响 L2 通道标签）
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

if __name__ == "__main__" or not __package__:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.feishu import send_feishu_notification

# ── 专属日报 webhook ──────────────────────────────────────────────
_REPORT_WEBHOOK = "https://open.feishu.cn/open-apis/bot/v2/hook/48748c19-aad6-457d-a0dd-212bb662901e"

TZ = ZoneInfo("Asia/Shanghai")


# ─────────────────────────────────────────────────────────────────
# 格式化工具
# ─────────────────────────────────────────────────────────────────

def _pct(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "N/A"
    return f"{numerator / denominator * 100:.1f}%"


def _bar(ratio: float, width: int = 10) -> str:
    """简单 ASCII 进度条，ratio 为 0~1。"""
    filled = max(0, min(width, round(ratio * width)))
    return "█" * filled + "░" * (width - filled)


def _regime_emoji(regime: str) -> str:
    return {
        "RISK_ON": "🟢",
        "NEUTRAL": "🟡",
        "RISK_OFF": "🟠",
        "CRASH": "🔴",
    }.get(str(regime).upper(), "⚪")


def _warn_if(condition: bool, msg: str) -> str:
    return f"⚠️ {msg}" if condition else ""


def _market_label(market: str) -> str:
    return {"cn": "A股", "hk": "港股", "us": "美股"}.get(
        str(market).lower(), market.upper()
    )


def _is_premarket(trade_date_str: str) -> bool | None:
    """判断当前调用是在 trade_date 盘前还是盘后，返回 True=盘前 / False=盘后 / None=无法判断。"""
    try:
        now = datetime.now(TZ)
        from datetime import date as _date
        td = _date.fromisoformat(trade_date_str)
        if now.date() == td:
            # 15:00 收盘
            return now.hour < 15
        if now.date() > td:
            return False  # 已是次日，肯定盘后
        return True  # 还没到交易日
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────
# 核心报告构建
# ─────────────────────────────────────────────────────────────────

def build_report_text(metrics: dict, *, run_ts: datetime | None = None) -> str:
    """
    根据 run_funnel_job 返回的 metrics 字典构建日报正文（飞书 Markdown 格式）。
    """
    now = run_ts or datetime.now(TZ)
    market = str(metrics.get("market", "cn") or "cn").lower()
    market_label = _market_label(market)

    # ── 时间信息 ──────────────────────────────────────────────────
    debug = metrics.get("_debug") or {}
    end_trade_date = str(debug.get("end_trade_date") or metrics.get("end_trade_date", "未知"))
    premarket = _is_premarket(end_trade_date)
    if premarket is True:
        session_label = "盘前"
    elif premarket is False:
        session_label = "盘后"
    else:
        session_label = "未知"

    run_ts_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # ── 股票池 ──────────────────────────────────────────────────
    total_pool = int(metrics.get("total_symbols", 0) or 0)
    fetch_ok = int(metrics.get("fetch_ok", 0) or 0)
    fetch_fail = int(metrics.get("fetch_fail", 0) or 0)
    integrity_pass = int(metrics.get("integrity_pass", 0) or 0)
    integrity_fail = int(metrics.get("integrity_fail", 0) or 0)
    fetch_elapsed = metrics.get("fetch_elapsed_s")  # 可能没有，来自 fetch_stats 注入

    # 缓存命中计数（需 stock_hist_repository 注入，默认 N/A）
    cache_hits = metrics.get("cache_hits")
    new_fetches = metrics.get("new_fetches")

    # ── 漏斗层 ──────────────────────────────────────────────────
    l0 = integrity_pass          # 完整性通过 → 进入漏斗
    l1 = int(metrics.get("layer1", 0) or 0)
    l2 = int(metrics.get("layer2", 0) or 0)
    l3 = int(metrics.get("layer3", 0) or 0)
    l4 = int(metrics.get("total_hits", 0) or 0)

    # L2 通道细分
    l2_momentum = int(metrics.get("layer2_momentum", 0) or 0)
    l2_ambush   = int(metrics.get("layer2_ambush", 0) or 0)
    l2_accum    = int(metrics.get("layer2_accum", 0) or 0)
    l2_dry_vol  = int(metrics.get("layer2_dry_vol", 0) or 0)
    l2_rs_div   = int(metrics.get("layer2_rs_div", 0) or 0)
    l2_sos      = int(metrics.get("layer2_sos", 0) or 0)

    # ── 大盘制度 ──────────────────────────────────────────────────
    bench = metrics.get("benchmark_context") or {}
    regime = str(bench.get("regime", "UNKNOWN") or "UNKNOWN").upper()
    regime_emoji = _regime_emoji(regime)
    main_today = bench.get("main_today_pct")
    main_today_str = f"{main_today:+.2f}%" if main_today is not None else "N/A"
    breadth = bench.get("breadth") or {}
    breadth_ratio = breadth.get("ratio_pct")
    breadth_str = f"{breadth_ratio:.1f}%" if breadth_ratio is not None else "N/A"
    bench_close = bench.get("close")
    bench_close_str = f"{bench_close:.2f}" if bench_close is not None else "N/A"

    # ── L1/L2 拒绝 Top ──────────────────────────────────────────
    l1_rej_top = metrics.get("layer1_rejection_top") or []
    l2_rej_top = metrics.get("layer2_rejection_top") or []

    def _fmt_rejection_top(top: list[dict]) -> str:
        if not top:
            return "无"
        return "、".join(
            f"{item.get('reason', '?')}={item.get('count', 0)}"
            for item in top[:5]
        )

    # ── L4 触发器 ──────────────────────────────────────────────
    by_trigger = metrics.get("by_trigger") or {}

    def _fmt_triggers(by_trigger: dict) -> str:
        if not any(by_trigger.values()):
            return "无命中"
        parts = [f"{k}={v}" for k, v in by_trigger.items() if v]
        return " | ".join(parts) if parts else "无命中"

    # ── 异常预警 ──────────────────────────────────────────────────
    warnings: list[str] = []
    if total_pool > 0 and fetch_fail / total_pool > 0.15:
        warnings.append(f"数据失败率 {_pct(fetch_fail, total_pool)} 超过 15%，请检查数据源")
    if fetch_ok > 0 and integrity_fail / fetch_ok > 0.05:
        warnings.append(f"完整性淘汰率 {_pct(integrity_fail, fetch_ok)} 超过 5%，可能存在大量次新股/停牌")
    if l0 > 0 and l1 / l0 < 0.3:
        warnings.append(f"L1 通过率 {_pct(l1, l0)} 异常低（< 30%），请检查 L1 阈值配置")
    if l1 > 0 and l2 / l1 < 0.02:
        warnings.append(f"L2 通过率 {_pct(l2, l1)} 异常低（< 2%），可能动量/趋势参数过严")
    if regime in ("CRASH", "RISK_OFF") and l4 > 10:
        warnings.append(f"制度={regime} 但 L4 命中 {l4} 只，请人工复核是否误触发")

    # ── 拼装正文 ──────────────────────────────────────────────────
    lines: list[str] = []

    # 标题行
    lines.append(f"**市场**: {market_label}　**交易日**: {end_trade_date}（{session_label}）")
    lines.append(f"**执行时间**: {run_ts_str}")
    lines.append("")

    # 大盘水温
    lines.append("**📊 大盘水温**")
    lines.append(
        f"制度 {regime_emoji} `{regime}` ｜ 今日 {main_today_str} ｜ 收盘 {bench_close_str}"
    )
    lines.append(f"面包量（站上MA{20}占比）: {breadth_str}")
    lines.append("")

    # 数据质量
    lines.append("**📦 数据拉取**")
    lines.append(f"股票池: **{total_pool}** 只")
    fail_rate = fetch_fail / total_pool if total_pool else 0
    lines.append(
        f"拉取成功: **{fetch_ok}** ✅ ｜ 失败: **{fetch_fail}** ❌（失败率 {_pct(fetch_fail, total_pool)}）"
    )
    lines.append(
        f"完整性通过: **{integrity_pass}** ✅ ｜ 淘汰: **{integrity_fail}** ❌（淘汰率 {_pct(integrity_fail, fetch_ok)}）"
    )
    if cache_hits is not None and new_fetches is not None:
        lines.append(f"缓存命中: **{cache_hits}** ｜ 新拉取: **{new_fetches}**")
    if fetch_elapsed is not None:
        lines.append(f"拉取耗时: {fetch_elapsed:.0f}s")
    lines.append("")

    # 漏斗漏斗漏斗
    lines.append("**🔻 漏斗层级**")

    def _layer_line(label: str, n_in: int, n_out: int, extra: str = "") -> str:
        ratio = n_out / n_in if n_in > 0 else 0.0
        bar = _bar(ratio, 8)
        pct_s = _pct(n_out, n_in)
        e = f" ｜ {extra}" if extra else ""
        return f"{label}　{n_in} → **{n_out}**　[{bar}] {pct_s}{e}"

    lines.append(_layer_line("数据→L1（成交/市值）", l0, l1))
    l2_breakdown = f"主升{l2_momentum} 潜伏{l2_ambush} 吸筹{l2_accum} 地量{l2_dry_vol} 护盘{l2_rs_div} 点火{l2_sos}"
    lines.append(_layer_line("L1→L2（动量/趋势）  ", l1, l2, l2_breakdown))
    lines.append(_layer_line("L2→L3（板块共振）    ", l2, l3))
    lines.append(_layer_line("L3→L4（威科夫触发）  ", l3, l4))
    lines.append(f"L4 触发器: {_fmt_triggers(by_trigger)}")
    lines.append("")

    # 拒绝 Top
    lines.append("**🔍 L1 拒绝原因 Top**")
    lines.append(_fmt_rejection_top(l1_rej_top))
    lines.append("")
    lines.append("**🔍 L2 拒绝原因 Top**")
    lines.append(_fmt_rejection_top(l2_rej_top))
    lines.append("")

    # 预警
    if warnings:
        lines.append("**⚠️ 异常预警**")
        for w in warnings:
            lines.append(f"• {w}")
        lines.append("")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# 对外 API：发送日报
# ─────────────────────────────────────────────────────────────────

def send_data_daily_report(
    metrics: dict,
    *,
    webhook: str | None = None,
    run_ts: datetime | None = None,
) -> bool:
    """
    根据 run_funnel_job 的 metrics 发送数据日报到飞书。

    参数
    ----
    metrics   : run_funnel_job 返回的 metrics 字典（需包含 _debug 以获取 end_trade_date）
    webhook   : 目标 webhook；None 时按优先级读取
                DATA_REPORT_WEBHOOK_URL → _REPORT_WEBHOOK → FEISHU_WEBHOOK_URL
    run_ts    : 执行时间（None = 当前时间）

    返回
    ----
    bool: 是否发送成功
    """
    url = (
        webhook
        or os.getenv("DATA_REPORT_WEBHOOK_URL", "").strip()
        or _REPORT_WEBHOOK
        or os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    )
    if not url:
        print("[report] 未配置任何 webhook，跳过日报发送")
        return False

    market = str(metrics.get("market", "cn") or "cn").lower()
    market_label = _market_label(market)
    debug = metrics.get("_debug") or {}
    end_trade_date = str(debug.get("end_trade_date") or metrics.get("end_trade_date", ""))
    title = f"📊 数据日报 · {market_label} · {end_trade_date}"
    content = build_report_text(metrics, run_ts=run_ts)

    print(f"[report] 发送数据日报: market={market}, date={end_trade_date}")
    ok = send_feishu_notification(url, title, content)
    print(f"[report] 日报发送: ok={ok}")
    return ok


# ─────────────────────────────────────────────────────────────────
# 独立运行入口：自动执行漏斗 + 出报告
# ─────────────────────────────────────────────────────────────────

def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="数据日报：运行漏斗并发送日报到飞书")
    parser.add_argument(
        "--market",
        default=os.getenv("FUNNEL_MARKET", "cn"),
        choices=["cn", "hk", "us"],
        help="市场（默认从 FUNNEL_MARKET 环境变量读取）",
    )
    parser.add_argument(
        "--webhook",
        default=None,
        help="飞书 webhook（默认使用 DATA_REPORT_WEBHOOK_URL 或内置地址）",
    )
    args = parser.parse_args()

    # 临时覆盖 FUNNEL_MARKET
    os.environ["FUNNEL_MARKET"] = args.market

    from core.funnel_pipeline import run_funnel_job

    run_ts = datetime.now(TZ)
    print(f"[report] 开始运行漏斗（market={args.market}）...")
    _triggers, metrics = run_funnel_job(include_debug_context=True)

    ok = send_data_daily_report(metrics, webhook=args.webhook, run_ts=run_ts)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
