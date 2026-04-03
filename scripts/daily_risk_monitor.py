# -*- coding: utf-8 -*-
"""
每日投资组合风控监控 (Daily Risk Monitor)

从 Supabase 读取当日持仓，计算：
1. 板块集中度（HHI 指数）
2. Trend/Accum 配比偏离
3. 大小盘集中度
4. 浮亏集中预警
5. 输出到飞书

用法:
    python -m scripts.daily_risk_monitor

环境变量:
    SUPABASE_URL, SUPABASE_KEY   - Supabase 连接
    FEISHU_WEBHOOK_URL            - 飞书推送
    TUSHARE_TOKEN                 - 获取行业和市值数据
    RISK_HHI_WARN_THRESHOLD       - 板块 HHI 预警阈值 (default: 0.3)
    RISK_TRACK_IMBALANCE_PCT      - Trend/Accum 偏离预警 (default: 70)
    RISK_LOSS_WARN_PCT            - 单票浮亏预警线 (default: -5.0)
"""
from __future__ import annotations

import os
import sys
from collections import Counter
from datetime import date

# ── 配置 ──
HHI_WARN = float(os.getenv("RISK_HHI_WARN_THRESHOLD", "0.3"))
TRACK_IMBALANCE_PCT = float(os.getenv("RISK_TRACK_IMBALANCE_PCT", "70.0"))
LOSS_WARN_PCT = float(os.getenv("RISK_LOSS_WARN_PCT", "-5.0"))
CAP_LARGE_THRESHOLD_YI = float(os.getenv("RISK_CAP_LARGE_YI", "500.0"))  # >500亿=大盘
CAP_SMALL_THRESHOLD_YI = float(os.getenv("RISK_CAP_SMALL_YI", "100.0"))  # <100亿=小盘


def calc_hhi(weights: list[float]) -> float:
    """Herfindahl-Hirschman Index：0~1，越高越集中。"""
    total = sum(weights)
    if total <= 0:
        return 0.0
    shares = [w / total for w in weights]
    return sum(s ** 2 for s in shares)


def _get_portfolio_positions() -> list[dict]:
    """从 Supabase 读取实盘持仓。"""
    try:
        from integrations.supabase_client import get_portfolio
        portfolio = get_portfolio()
        if portfolio is None:
            return []
        return [
            {
                "code": str(p.code).strip(),
                "name": str(p.name).strip(),
                "strategy": str(getattr(p, "strategy", "")).strip(),
                "current_price": float(getattr(p, "current_price", 0) or 0),
                "avg_cost": float(getattr(p, "avg_cost", 0) or 0),
                "shares": int(getattr(p, "shares", 0) or 0),
                "market_value": float(getattr(p, "market_value", 0) or 0),
            }
            for p in portfolio.positions
            if hasattr(p, "code") and p.code
        ]
    except Exception as exc:
        print(f"[risk_monitor] 获取持仓失败: {exc}")
        return []


def _infer_track(strategy: str) -> str:
    s = strategy.lower()
    if any(k in s for k in ("trend", "markup", "sos", "主升", "点火", "突破")):
        return "Trend"
    if any(k in s for k in ("accum", "spring", "lps", "潜伏", "吸筹", "地量")):
        return "Accum"
    return "Unknown"


def run_risk_check(positions: list[dict]) -> dict:
    """执行全量风控检查，返回报告数据。"""
    alerts: list[str] = []
    metrics: dict = {}

    if not positions:
        return {"alerts": ["⚠️ 当前无持仓"], "metrics": {}}

    total_value = sum(p.get("market_value", 0) for p in positions)
    metrics["total_positions"] = len(positions)
    metrics["total_value"] = total_value

    # ── 1. 板块集中度 ──
    try:
        from integrations.data_source import fetch_sector_map
        sector_map = fetch_sector_map()
    except Exception:
        sector_map = {}

    sector_values: dict[str, float] = {}
    for p in positions:
        sector = sector_map.get(p["code"], "未知")
        sector_values[sector] = sector_values.get(sector, 0) + p.get("market_value", 0)

    if sector_values:
        hhi = calc_hhi(list(sector_values.values()))
        metrics["sector_hhi"] = hhi
        metrics["sector_distribution"] = dict(
            sorted(sector_values.items(), key=lambda x: -x[1])
        )
        if hhi > HHI_WARN:
            top_sector = max(sector_values, key=sector_values.get)
            top_pct = sector_values[top_sector] / total_value * 100 if total_value > 0 else 0
            alerts.append(
                f"🔴 板块集中度预警: HHI={hhi:.3f} > {HHI_WARN}，"
                f"最重仓板块「{top_sector}」占 {top_pct:.1f}%"
            )

    # ── 2. Trend / Accum 配比 ──
    track_values: dict[str, float] = {"Trend": 0, "Accum": 0, "Unknown": 0}
    for p in positions:
        track = _infer_track(p.get("strategy", ""))
        track_values[track] = track_values.get(track, 0) + p.get("market_value", 0)

    known_total = track_values["Trend"] + track_values["Accum"]
    if known_total > 0:
        trend_pct = track_values["Trend"] / known_total * 100
        accum_pct = track_values["Accum"] / known_total * 100
        metrics["trend_pct"] = trend_pct
        metrics["accum_pct"] = accum_pct
        if trend_pct > TRACK_IMBALANCE_PCT:
            alerts.append(
                f"🟡 Trend 轨过重: {trend_pct:.1f}% > {TRACK_IMBALANCE_PCT}%，"
                f"熊市中可能回撤较大"
            )
        if accum_pct > TRACK_IMBALANCE_PCT:
            alerts.append(
                f"🟡 Accum 轨过重: {accum_pct:.1f}% > {TRACK_IMBALANCE_PCT}%，"
                f"牛市中可能跑输"
            )

    # ── 3. 大小盘集中度 ──
    try:
        from integrations.data_source import fetch_market_cap_map
        cap_map = fetch_market_cap_map()
    except Exception:
        cap_map = {}

    cap_groups = {"大盘": 0.0, "中盘": 0.0, "小盘": 0.0, "未知": 0.0}
    for p in positions:
        cap_yi = cap_map.get(p["code"], 0) / 1e8 if cap_map.get(p["code"]) else 0
        mv = p.get("market_value", 0)
        if cap_yi >= CAP_LARGE_THRESHOLD_YI:
            cap_groups["大盘"] += mv
        elif cap_yi >= CAP_SMALL_THRESHOLD_YI:
            cap_groups["中盘"] += mv
        elif cap_yi > 0:
            cap_groups["小盘"] += mv
        else:
            cap_groups["未知"] += mv
    metrics["cap_distribution"] = {k: v for k, v in cap_groups.items() if v > 0}

    small_pct = cap_groups["小盘"] / total_value * 100 if total_value > 0 else 0
    if small_pct > 60:
        alerts.append(f"🟡 小盘股占比 {small_pct:.1f}% > 60%，流动性风险较高")

    # ── 4. 单票浮亏预警 ──
    loss_positions = []
    for p in positions:
        cost = p.get("avg_cost", 0)
        price = p.get("current_price", 0)
        if cost > 0 and price > 0:
            pnl_pct = (price - cost) / cost * 100
            if pnl_pct <= LOSS_WARN_PCT:
                loss_positions.append({
                    "code": p["code"],
                    "name": p["name"],
                    "pnl_pct": pnl_pct,
                    "value": p.get("market_value", 0),
                })
    if loss_positions:
        metrics["loss_positions"] = loss_positions
        loss_names = ", ".join(
            f"{lp['code']}{lp['name']}({lp['pnl_pct']:.1f}%)"
            for lp in sorted(loss_positions, key=lambda x: x["pnl_pct"])[:5]
        )
        alerts.append(f"🔴 浮亏预警: {len(loss_positions)} 只超过 {LOSS_WARN_PCT}% → {loss_names}")

    if not alerts:
        alerts.append("✅ 所有风控指标正常")

    return {"alerts": alerts, "metrics": metrics}


def format_risk_report(check_result: dict) -> str:
    alerts = check_result.get("alerts", [])
    metrics = check_result.get("metrics", {})

    lines = [
        f"# 📊 每日风控监控 ({date.today().isoformat()})",
        "",
        "## 预警",
    ]
    for a in alerts:
        lines.append(f"- {a}")

    lines.extend(["", "## 持仓概览"])
    lines.append(f"- 持仓数: {metrics.get('total_positions', 0)}")
    lines.append(f"- 总市值: {metrics.get('total_value', 0):,.0f}")

    if "trend_pct" in metrics:
        lines.append(f"- Trend/Accum: {metrics['trend_pct']:.1f}% / {metrics.get('accum_pct', 0):.1f}%")

    if "sector_hhi" in metrics:
        lines.append(f"- 板块 HHI: {metrics['sector_hhi']:.3f}")

    sector_dist = metrics.get("sector_distribution", {})
    if sector_dist:
        lines.extend(["", "## 板块分布"])
        total = sum(sector_dist.values())
        for sector, val in list(sector_dist.items())[:8]:
            pct = val / total * 100 if total > 0 else 0
            lines.append(f"- {sector}: {pct:.1f}%")

    cap_dist = metrics.get("cap_distribution", {})
    if cap_dist:
        lines.extend(["", "## 大小盘分布"])
        total = sum(cap_dist.values())
        for cap_type, val in cap_dist.items():
            pct = val / total * 100 if total > 0 else 0
            lines.append(f"- {cap_type}: {pct:.1f}%")

    return "\n".join(lines)


def main() -> int:
    print(f"[risk_monitor] {date.today().isoformat()} 开始执行风控监控")
    positions = _get_portfolio_positions()
    print(f"[risk_monitor] 获取到 {len(positions)} 个持仓")

    result = run_risk_check(positions)
    report = format_risk_report(result)
    print(report)

    # 推送飞书
    webhook = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    if webhook:
        try:
            from utils.feishu import send_feishu_notification
            sent = send_feishu_notification(
                webhook,
                f"📊 风控监控 {date.today().isoformat()}",
                report,
            )
            print(f"[risk_monitor] 飞书推送: {'成功' if sent else '失败'}")
        except Exception as exc:
            print(f"[risk_monitor] 飞书推送异常: {exc}")

    # 写本地文件
    logs_dir = os.getenv("LOGS_DIR", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    out_path = os.path.join(logs_dir, f"risk_monitor_{date.today().strftime('%Y%m%d')}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"[risk_monitor] 报告 -> {out_path}")

    # 有严重预警时返回非零退出码
    has_critical = any("🔴" in a for a in result.get("alerts", []))
    return 1 if has_critical else 0


if __name__ == "__main__":
    raise SystemExit(main())
