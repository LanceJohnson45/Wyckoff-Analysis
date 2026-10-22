"""推送纯数据更新结果；不运行分析，不把缺失摘要或发送失败当作成功。"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
from urllib.request import Request, urlopen

if __name__ == "__main__" and not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import structlog

_STATUS_LABELS = {"gap_repaired": "本轮补齐", "manifest_cached": "检查点已完成", "cache_ready": "缓存完整",
                  "suspended": "确认停牌", "excluded": "排除不更新", "not_listed": "尚未上市",
                  "pending": "尚未完成", "failed": "失败", "gap_partial": "部分缺失"}


@dataclass(frozen=True)
class DataNotice:
    """数据任务通知的可审阅内容。"""

    title: str
    content: str


def build_notice(directory: Path, market: str, run_status: str, run_url: str) -> DataNotice:
    """从本次工作流产物生成通知，失败任务有旧成功摘要时仍明确提示失败。"""
    paths = list(directory.glob(f"prewarm_{market}_*_summary.json"))
    if paths:
        path = max(paths, key=lambda item: item.stat().st_mtime_ns)
        summary = json.loads(path.read_text(encoding="utf-8"))
        if summary.get("market") != market:
            raise ValueError("summary market mismatch")
        counts = summary["counts"]
        if not isinstance(counts, dict) or any(type(value) is not int or value < 0 for value in counts.values()):
            raise ValueError("invalid summary counts")
        total = summary["total"]
        if type(total) is not int or total != sum(counts.values()):
            raise ValueError("summary counts do not match total")
        ready = summary.get("status") == "ok" and not summary.get("universe_warning") and not any(counts.get(key, 0) for key in ("pending", "failed", "gap_partial", "dry_run_missing"))
        success = run_status == "success" and ready
        label = "✅ 成功" if success else "⚠️ 未完整完成"
        target = summary["target_date"]
        lines = [f"目标交易日：{target}", f"任务结果：{label}", f"股票总数：{total}"]
        lines.extend(f"{_STATUS_LABELS.get(key, key)}：{value}" for key, value in sorted(counts.items()))
        if summary.get("universe_warning"):
            lines.append("证券名单存在警告，详情见运行产物。")
    else:
        target = "目标日未生成"
        label = "❌ 未取得更新摘要"
        lines = ["本次任务未生成数据覆盖摘要，请检查准备阶段和取数日志。", f"工作流状态：{run_status}"]
    lines.extend([f"[查看运行日志与审核文件]({run_url})", "统计为本次任务结果；缓存跳过与新补齐分别计数。"])
    return DataNotice(f"📊 {market.upper()} 数据更新 · {target} · {label}", "\n".join(lines))


def send_notice(webhook: str, notice: DataNotice) -> None:
    """发送飞书卡片，必须收到明确的成功业务码，不记录Webhook地址。"""
    payload = {"msg_type": "interactive", "card": {
        "header": {"title": {"tag": "plain_text", "content": notice.title}},
        "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": notice.content}}],
    }}
    request = Request(webhook, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                      headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=15) as response:
        result = json.loads(response.read())
    code = result.get("code", result.get("StatusCode"))
    if type(code) is not int or code != 0:
        raise RuntimeError("feishu notification rejected or missing success code")


def main() -> int:
    """通过环境注入Webhook，失败返回非零以便Actions展示真实通知状态。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary-dir", type=Path, default=Path("logs"))
    parser.add_argument("--market", choices=["cn", "us", "hk"], required=True)
    parser.add_argument("--run-status", choices=["success", "failure", "cancelled"], required=True)
    parser.add_argument("--run-url", required=True)
    args = parser.parse_args()
    logger = structlog.get_logger()
    webhook = os.getenv("DATA_REPORT_WEBHOOK_URL", "").strip() or os.getenv("FEISHU_WEBHOOK_URL", "").strip()
    if not webhook:
        logger.error("market_data_notification_unconfigured", market=args.market)
        return 2
    try:
        notice = build_notice(args.summary_dir, args.market, args.run_status, args.run_url)
        send_notice(webhook, notice)
    except Exception as exc:
        # 网络异常的字符串可能含Webhook地址，因此仅记录异常类型。
        logger.error("market_data_notification_failed", market=args.market, error_type=type(exc).__name__)
        return 2
    logger.info("market_data_notification_sent", market=args.market, run_status=args.run_status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
