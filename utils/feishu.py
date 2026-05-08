# -*- coding: utf-8 -*-
"""
飞书 Webhook 通知，纯工具函数。

配置来源由调用方决定，互不耦合：
- Streamlit：使用用户登录后 Supabase 中的 feishu_webhook
- 定时任务：使用 GitHub Actions 的 FEISHU_WEBHOOK_URL secret
"""
from __future__ import annotations

import os
import re
import requests
import time


_TERM_GLOSSARY_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Regime / risk state
    (re.compile(r"\bBLACK_SWAN\b(?!\s*[（(])"), "BLACK_SWAN（黑天鹅高风险）"),
    (re.compile(r"\bRISK_OFF\b(?!\s*[（(])"), "RISK_OFF（风险收缩）"),
    (re.compile(r"\bRISK_ON\b(?!\s*[（(])"), "RISK_ON（风险偏好）"),
    (re.compile(r"\bNORMAL\b(?!\s*[（(])"), "NORMAL（常态）"),
    (re.compile(r"\bPANIC_REPAIR\b(?!\s*[（(])"), "PANIC_REPAIR（恐慌修复）"),
    # Macro / market indicators
    (re.compile(r"\bVIX\b(?!\s*[（(])"), "VIX（波动率恐慌指数）"),
    (re.compile(r"\bA50\b(?!\s*[（(])"), "A50（富时中国A50期货）"),
    (re.compile(r"\bATR\b(?!\s*[（(])"), "ATR（真实波动幅度）"),
    (re.compile(r"\bRPS\b(?!\s*[（(])"), "RPS（相对强弱百分位）"),
    (re.compile(r"\bQPS\b(?!\s*[（(])"), "QPS（每秒请求量）"),
    # OMS actions
    (re.compile(r"\bFULL_ATTACK\b(?!\s*[（(])"), "FULL_ATTACK（全仓进攻）"),
    (re.compile(r"\bLIGHT_ADD\b(?!\s*[（(])"), "LIGHT_ADD（轻量加仓）"),
    (re.compile(r"\bATTACK\b(?!\s*[（(])"), "ATTACK（进攻建仓）"),
    (re.compile(r"\bPROBE\b(?!\s*[（(])"), "PROBE（试探建仓）"),
    (re.compile(r"\bTRIM\b(?!\s*[（(])"), "TRIM（减仓）"),
    (re.compile(r"\bHOLD\b(?!\s*[（(])"), "HOLD（持有观察）"),
    (re.compile(r"\bEXIT\b(?!\s*[（(])"), "EXIT（清仓离场）"),
    (re.compile(r"\bNO_TRADE\b(?!\s*[（(])"), "NO_TRADE（拒单）"),
    (re.compile(r"\bAPPROVED\b(?!\s*[（(])"), "APPROVED（核准执行）"),
    # Wyckoff terms
    (re.compile(r"\bComposite Man\b(?!\s*[（(])"), "Composite Man（综合人/主力）"),
    (re.compile(r"\bTape Reading\b(?!\s*[（(])"), "Tape Reading（盘面解读）"),
    (re.compile(r"\bSpring\b(?!\s*[（(])"), "Spring（弹簧/假跌破）"),
    (re.compile(r"\bLPS\b(?!\s*[（(])"), "LPS（最后支撑点）"),
    (re.compile(r"\bSOS\b(?!\s*[（(])"), "SOS（强势信号）"),
    (re.compile(r"\bUTAD\b(?!\s*[（(])"), "UTAD（上冲诱多）"),
    (re.compile(r"\bEVR\b(?!\s*[（(])"), "EVR（放量不跌）"),
    (re.compile(r"\bJAC\b(?!\s*[（(])"), "JAC（跃过小溪）"),
    (re.compile(r"\bBUEC\b(?!\s*[（(])"), "BUEC（回踩小溪边缘）"),
    # Common trade terms
    (re.compile(r"\bStop[- ]?Loss\b(?!\s*[（(])", re.IGNORECASE), "Stop-Loss（止损位）"),
    (re.compile(r"\bEntry\b(?!\s*[（(])", re.IGNORECASE), "Entry（入场区）"),
    (re.compile(r"\bTarget\b(?!\s*[（(])", re.IGNORECASE), "Target（目标位）"),
]


def _annotate_financial_terms(content: str) -> str:
    """
    将常见金融英文术语补充为“术语（中文解释）”，提升飞书可读性。
    已带括号解释的术语会跳过，避免重复注释。
    """
    if not content:
        return content
    out = content
    for pattern, replacement in _TERM_GLOSSARY_PATTERNS:
        out = pattern.sub(replacement, out)
    return out


def _normalize_for_lark_md(content: str) -> str:
    """
    飞书 lark_md 不是完整 Markdown：
    - 标题 '#' 在卡片里常不按标题渲染
    - 分割线 '---' 会出现为普通文本
    - 特殊符号 '<', '>' 如果不转义，会导致飞书客户端解析失败、卡片完全吞掉不显示。
    这里做轻量归一化，保证展示稳定。
    """
    # 转义尖括号，防止客户端渲染引擎崩溃（API 会返回 0，但在群里没卡片）
    safe_content = content.replace("<", "&lt;").replace(">", "&gt;")
    lines = safe_content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    for raw in lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            out.append("")
            continue
        if stripped.startswith("#"):
            title = stripped.lstrip("#").strip()
            out.append(f"**{title}**" if title else "")
            continue
        if stripped in {"---", "***", "___"}:
            out.append("")
            continue
        out.append(line)
    return "\n".join(out).strip()


def _split_lark_md(content: str, max_len: int = 2800) -> list[str]:
    """
    飞书卡片单个 lark_md 文本体积有限，长文按段分片。
    """
    if len(content) <= max_len:
        return [content]

    paragraphs = content.split("\n\n")
    chunks: list[str] = []
    current = ""
    for p in paragraphs:
        candidate = p if not current else f"{current}\n\n{p}"
        if len(candidate) <= max_len:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(p) <= max_len:
            current = p
            continue
        start = 0
        while start < len(p):
            chunks.append(p[start:start + max_len])
            start += max_len
    if current:
        chunks.append(current)
    return chunks


def _post_card(webhook_url: str, title: str, chunk: str) -> tuple[bool, str]:
    headers = {"Content-Type": "application/json"}
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": chunk}}
            ],
        },
    }
    resp = requests.post(webhook_url.strip(), headers=headers, json=payload, timeout=10)
    if resp.status_code != 200:
        return (False, f"http_{resp.status_code}")
    try:
        data = resp.json()
        code = int(data.get("code", -1))
        if code == 0:
            return (True, "ok")
        return (False, f"feishu_code_{code}: {data.get('msg', '')}")
    except Exception:
        return (True, "ok_non_json")


def _post_rich_card(
    webhook_url: str, title: str, elements: list, template: str = "blue",
) -> tuple[bool, str]:
    """发送飞书富文本卡片（column_set 等原生组件）。"""
    headers = {"Content-Type": "application/json"}
    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": template,
            },
            "elements": elements,
        },
    }
    resp = requests.post(webhook_url.strip(), headers=headers, json=payload, timeout=15)
    if resp.status_code != 200:
        return (False, f"http_{resp.status_code}")
    try:
        data = resp.json()
        code = int(data.get("code", -1))
        if code == 0:
            return (True, "ok")
        return (False, f"feishu_code_{code}: {data.get('msg', '')}")
    except Exception:
        return (True, "ok_non_json")


def send_backtest_card(webhook_url: str, summary_path: str) -> bool:
    """解析 backtest summary markdown，用飞书原生 column_set 组件发送美观卡片。"""
    if not webhook_url or not webhook_url.strip():
        return False

    with open(summary_path, "r", encoding="utf-8") as f:
        content = f.read()

    def _extract(keyword: str) -> float | None:
        for line in content.split("\n"):
            if keyword in line:
                val = line.split(":")[-1].strip().rstrip("%")
                try:
                    return float(val)
                except ValueError:
                    return None
        return None

    def _extract_str(keyword: str) -> str:
        for line in content.split("\n"):
            if keyword in line:
                return line.split(":")[-1].strip()
        return "?"

    # --- 解析基本指标 ---
    win = _extract("胜率")
    avg = _extract("平均收益")
    sharpe = _extract("夏普比")
    mdd = _extract("最大回撤")
    calmar = _extract("卡玛比")
    trades = _extract("成交样本")
    hold = _extract_str("持有周期")
    exit_mode = _extract_str("离场模式")
    sl = _extract_str("止损线")
    tp = _extract_str("止盈线")
    trail = _extract_str("移动止盈")
    bt_range = _extract_str("区间")
    top_n = _extract_str("每日候选上限")
    pool = _extract_str("股票池")

    def _fmt(val, spec):
        return format(val, spec) if val is not None else "-"

    # --- 解析竖向分层表 ---
    def _parse_vertical_table(header_keyword: str):
        """解析 | 指标 | ColA | ColB | ... | 格式的竖向表。"""
        lines = content.split("\n")
        result = {}
        in_table = False
        col_names = []
        for line in lines:
            if header_keyword in line and not in_table:
                in_table = True
                continue
            if in_table and line.startswith("| 指标"):
                col_names = [c.strip() for c in line.split("|")[2:-1]]
                for cn in col_names:
                    result[cn] = {}
                continue
            if in_table and line.startswith("|--"):
                continue
            if in_table and line.startswith("| "):
                parts = [p.strip() for p in line.split("|")[1:-1]]
                if len(parts) >= len(col_names) + 1:
                    key = parts[0]
                    for i, cn in enumerate(col_names):
                        result[cn][key] = parts[i + 1]
                continue
            if in_table and (line.startswith("##") or line.strip() == ""):
                if col_names:
                    break
        return result

    track_data = _parse_vertical_table("Trend vs Accum")
    regime_data = _parse_vertical_table("按大盘水温")

    # --- 构建卡片元素 ---
    elements = []

    # 摘要
    elements.append({"tag": "div", "text": {"tag": "lark_md",
        "content": f"**区间** {bt_range}  ·  **TopN** {top_n}  ·  **{pool}**"}})
    elements.append({"tag": "div", "text": {"tag": "lark_md",
        "content": f"📌 **参数**  持有{hold} / SL{sl} / TP{tp} / 移动止盈{trail}"}})
    elements.append({"tag": "hr"})

    # 核心指标
    sharpe_val = sharpe or 0
    tag = "🏆" if sharpe_val > 0 else "📌"
    cols = [
        ("**夏普比**", f"{tag} {_fmt(sharpe, '.3f')}"),
        ("**胜率**", f"{_fmt(win, '.1f')}%"),
        ("**均收**", f"{_fmt(avg, '+.2f')}%"),
        ("**回撤**", f"{_fmt(mdd, '.1f')}%"),
        ("**样本**", f"{int(trades or 0)}笔"),
    ]
    elements.append({"tag": "column_set", "flex_mode": "stretch",
        "background_style": "grey", "columns": [
            {"tag": "column", "width": "weighted", "weight": 1, "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"{label}\n{val}"}}
            ]} for label, val in cols
        ]})
    elements.append({"tag": "hr"})

    # 分轨统计
    if track_data:
        elements.append({"tag": "div", "text": {"tag": "lark_md",
            "content": "**分轨统计 (Trend vs Accum)**"}})
        track_cols = []
        icons = {"Trend": "⚡", "Accum": "🔄"}
        for name in track_data:
            d = track_data[name]
            txt = (
                f"{icons.get(name, '·')} **{name}**\n"
                f"{d.get('成交笔数', '?')}笔 · 胜率{d.get('胜率(%)', '?')}%\n"
                f"均收{d.get('平均收益(%)', '?')}% · 夏普{d.get('夏普比', '?')}\n"
                f"连亏{d.get('最长连亏', '?')}笔"
            )
            track_cols.append({"tag": "column", "width": "weighted", "weight": 1,
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": txt}}]})
        elements.append({"tag": "column_set", "flex_mode": "stretch", "columns": track_cols})
        elements.append({"tag": "hr"})

    # 按水温
    if regime_data:
        elements.append({"tag": "div", "text": {"tag": "lark_md",
            "content": "**按大盘水温**"}})
        regime_icons = {
            "NEUTRAL": "🟡", "PANIC_REPAIR": "🟠", "RISK_OFF": "🔴",
            "RISK_ON": "🟢", "CRASH": "⚫",
        }
        regime_cols = []
        for name in regime_data:
            d = regime_data[name]
            short = name.replace("PANIC_REPAIR", "PANIC")
            txt = (
                f"{regime_icons.get(name, '·')} **{short}**\n"
                f"{d.get('成交笔数', '?')}笔 · 胜率{d.get('胜率(%)', '?')}%\n"
                f"均收{d.get('平均收益(%)', '?')}%"
            )
            regime_cols.append({"tag": "column", "width": "weighted", "weight": 1,
                "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": txt}}]})
        elements.append({"tag": "column_set", "flex_mode": "stretch", "columns": regime_cols})

    # --- 发送 ---
    template = "blue" if sharpe_val > 0 else "orange"
    title = "📊 Backtest 回测报告"
    try:
        ok = False
        last_err = "unknown"
        for attempt in range(1, 4):
            ok, err = _post_rich_card(webhook_url, title, elements, template)
            if ok:
                print(f"[feishu] backtest card sent, attempt={attempt}")
                return True
            last_err = err
            time.sleep(0.6 * attempt)
        print(f"[feishu] backtest card failed: {last_err}")
        return False
    except Exception as e:
        print(f"[feishu] backtest card error: {e}")
        return False


def send_feishu_notification(webhook_url: str, title: str, content: str) -> bool:
    """发送飞书卡片消息。webhook_url 由调用方传入，为空时返回 False。"""
    if not webhook_url or not webhook_url.strip():
        return False

    annotated = _annotate_financial_terms(content)
    normalized = _normalize_for_lark_md(annotated)
    max_len = int(os.getenv("FEISHU_LARK_MAX_LEN", "2800"))
    chunks = _split_lark_md(normalized, max_len=max_len)

    try:
        total = len(chunks)
        for idx, chunk in enumerate(chunks, start=1):
            part_title = title if total == 1 else f"{title} ({idx}/{total})"
            ok = False
            last_err = "unknown"
            for attempt in range(1, 4):
                ok, err = _post_card(webhook_url, part_title, chunk)
                if ok:
                    print(f"[feishu] sent part {idx}/{total}, len={len(chunk)}, attempt={attempt}")
                    break
                last_err = err
                sleep_s = 0.6 * attempt
                print(
                    f"[feishu] failed part {idx}/{total}, len={len(chunk)}, "
                    f"attempt={attempt}, err={err}, retry_in={sleep_s:.1f}s"
                )
                time.sleep(sleep_s)
            if not ok:
                print(f"Feishu notification failed on part {idx}/{total}: {last_err}")
                return False
            if idx < total:
                time.sleep(0.15)
        return True
    except Exception as e:
        print(f"Feishu notification failed: {e}")
        return False
