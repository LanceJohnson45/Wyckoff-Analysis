from __future__ import annotations

from collections import Counter
from datetime import date

from scripts.review_list_replay import (
    _build_focus_lines,
    _build_report_lines,
    _format_recommendation_history,
    _normalize_code6,
    _short_code_list,
)


def _row(code: str, name: str, stage: str) -> dict[str, str]:
    return {"code": code, "name": name, "stage": stage, "reason": ""}


def test_short_code_list_limits_output():
    rows = [
        _row("000001", "平安银行", "L2淘汰"),
        _row("000002", "万科A", "L2淘汰"),
        _row("000003", "国农科技", "L2淘汰"),
    ]

    assert _short_code_list(rows, limit=2) == "000001平安银行、000002万科A、等3只"


def test_build_focus_lines_highlights_actionable_buckets():
    rows = [
        _row("000001", "平安银行", "L2淘汰"),
        _row("000002", "万科A", "L2淘汰"),
        _row("000003", "国农科技", "风控淘汰[触发结构止损或派发]"),
        _row("000004", "长江证券", "L4未命中"),
        _row("000005", "世纪星源", "L3淘汰"),
        _row("000006", "深振业A", "L1淘汰"),
        _row("000007", "全新好", "L4命中"),
    ]

    lines = _build_focus_lines(rows, today=date(2026, 5, 6), previous_trade_date=date(2026, 4, 30))
    text = "\n".join(lines)

    assert lines[0] == "**重点归因**"
    assert "日期间隔" in text
    assert "L2 是主因" in text
    assert "风控冲突优先复盘" in text
    assert "000003国农科技" in text
    assert "L4 扳机漏网" in text
    assert "板块层漏网" in text
    assert "基础过滤漏网" in text
    assert "已被漏斗捕获" in text


def test_format_recommendation_history_reports_missing_and_hits():
    assert _normalize_code6(1) == "000001"
    assert _format_recommendation_history("000001", {}) == "推荐记录: 此股没被推荐过"

    lookup = {
        "000001": [
            {"code": 1, "recommend_date": 20260430, "recommend_count": 3},
            {"code": 1, "recommend_date": 20260429, "recommend_count": 2},
        ]
    }

    note = _format_recommendation_history("000001", lookup)

    assert "2026-04-30、2026-04-29 被推荐过" in note
    assert "累计推荐3次" in note


def test_build_report_lines_appends_recommendation_note():
    rows = [
        {
            "code": "000001",
            "name": "平安银行",
            "stage": "L2淘汰",
            "reason": "六通道均未通过",
            "recommendation": "推荐记录: 2026-04-30 被推荐过；累计推荐1次",
        }
    ]

    lines = _build_report_lines(
        rows,
        Counter({"L2淘汰": 1}),
        today=date(2026, 5, 6),
        previous_trade_date=date(2026, 4, 30),
        end_trade_date="2026-04-30",
    )

    assert "推荐记录: 2026-04-30 被推荐过；累计推荐1次" in "\n".join(lines)
    assert "**推荐表交叉检查**: 命中1只 | 未推荐0只" in "\n".join(lines)
