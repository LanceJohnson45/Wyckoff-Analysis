# -*- coding: utf-8 -*-
"""
工具注册表 — 复用 agents/chat_tools.py 的 10 个函数，去除 ADK 依赖。

核心思路：
1. ToolContext 用 shim 类替代（只需 .state 属性）
2. 工具 JSON Schema 手动定义（比自动生成更可控）
3. 凭证通过 .env 环境变量提供
"""
from __future__ import annotations

import inspect
import json
import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ToolContext shim — 替代 google.adk.tools.ToolContext
# ---------------------------------------------------------------------------

class ToolContext:
    """最小化 ToolContext shim，只提供 .state 属性。"""

    def __init__(self, state: dict[str, Any] | None = None):
        self.state = state or {}


# ---------------------------------------------------------------------------
# 工具 Schema 定义（标准 JSON Schema，三家 Provider 通用）
# ---------------------------------------------------------------------------

TOOL_SCHEMAS: list[dict[str, Any]] = [
    {
        "name": "search_stock_by_name",
        "description": "根据关键词搜索 A 股股票，支持名称、代码、拼音首字母模糊搜索。最多返回 10 条。",
        "parameters": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "搜索关键词，如 '宁德' 或 '300750' 或 'gzmt'"},
            },
            "required": ["keyword"],
        },
    },
    {
        "name": "diagnose_stock",
        "description": "对单只 A 股股票做 Wyckoff 结构化健康诊断。包括均线结构、通道分类、吸筹阶段、触发信号（SOS/Spring/LPS/EVR）、退出信号、止损状态等。",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "6 位股票代码，如 '000001' 或 '600519'"},
                "cost": {"type": "number", "description": "持仓成本价，默认 0 表示未持仓"},
            },
            "required": ["code"],
        },
    },
    {
        "name": "diagnose_portfolio",
        "description": "诊断当前用户所有持仓的健康状况。从 Supabase 加载用户持仓，对每只股票运行 Wyckoff 健康诊断。",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_stock_price",
        "description": "获取指定股票的近期行情数据（OHLCV + 涨跌幅）。",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "6 位股票代码"},
                "days": {"type": "integer", "description": "获取天数，默认 30，最大 250"},
            },
            "required": ["code"],
        },
    },
    {
        "name": "get_market_overview",
        "description": "获取 A 股大盘环境概览，返回上证、深证、创业板等主要指数的最新收盘和涨跌幅。",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "screen_stocks",
        "description": "运行 Wyckoff 五层漏斗筛选，从全市场筛选出具有结构性机会的股票。整个过程可能需要几分钟。",
        "parameters": {
            "type": "object",
            "properties": {
                "board": {
                    "type": "string",
                    "description": "股票池板块：'all'（全部）、'main'（主板）、'chinext'（创业板）",
                },
            },
        },
    },
    {
        "name": "generate_ai_report",
        "description": "对指定股票列表生成威科夫三阵营 AI 深度研报（逻辑破产/储备营地/起跳板）。需要 Gemini API Key。最多 10 只。",
        "parameters": {
            "type": "object",
            "properties": {
                "stock_codes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "股票代码列表，如 ['000001', '600519']",
                },
            },
            "required": ["stock_codes"],
        },
    },
    {
        "name": "generate_strategy_decision",
        "description": "综合持仓和候选标的，生成去留决策（EXIT/TRIM/HOLD/PROBE/ATTACK）。需要 Gemini API Key 和持仓数据。",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_recommendation_tracking",
        "description": "查询最近的 AI 推荐记录及其后续涨跌幅表现。",
        "parameters": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "返回记录数，默认 20，最大 50"},
            },
        },
    },
    {
        "name": "get_signal_pending",
        "description": "查询信号确认池（signal_pending）。L4 触发信号经 1-3 天价格确认后变为 confirmed（可操作）或 expired（失效）。",
        "parameters": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "筛选状态：'all'（全部）、'pending'（待确认）、'confirmed'（已确认）、'expired'（已过期），默认 'all'",
                },
                "limit": {"type": "integer", "description": "返回记录数，默认 30，最大 100"},
            },
        },
    },
    {
        "name": "get_portfolio",
        "description": "查看用户当前持仓列表和可用资金。仅返回原始数据，不做诊断分析。用户问'我有什么持仓''持仓列表'时调用此工具。",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "update_portfolio",
        "description": "管理用户持仓：新增、修改、删除持仓，或设置可用资金。操作后返回最新持仓状态。",
        "parameters": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add", "update", "remove", "set_cash"],
                    "description": "操作类型：add（新增/加仓）、update（修改持仓信息）、remove（删除持仓）、set_cash（设置可用资金）",
                },
                "code": {"type": "string", "description": "6 位股票代码（add/update/remove 时必填）"},
                "name": {"type": "string", "description": "股票名称（可选）"},
                "shares": {"type": "integer", "description": "持仓股数"},
                "cost_price": {"type": "number", "description": "成本价"},
                "buy_dt": {"type": "string", "description": "买入日期（YYYYMMDD 格式）"},
                "free_cash": {"type": "number", "description": "可用资金（set_cash 时使用）"},
            },
            "required": ["action"],
        },
    },
    {
        "name": "check_background_tasks",
        "description": "查询后台任务执行状态。用户问'扫描好了没''任务进度'时调用。",
        "parameters": {
            "type": "object",
            "properties": {},
        },
    },
    # ── Agent 标准工具 ──
    {
        "name": "exec_command",
        "description": "在用户本地执行 shell 命令并返回输出。可用于安装软件、查看系统状态、运行脚本等。",
        "parameters": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "要执行的 shell 命令"},
                "timeout": {"type": "integer", "description": "超时秒数，默认 30，最大 120"},
            },
            "required": ["command"],
        },
    },
    {
        "name": "read_file",
        "description": "读取用户本地文件内容。支持 txt/csv/json/xlsx 等格式。用户发来文件路径时使用此工具。CSV/Excel 自动解析为表格预览（前 50 行）。",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "文件路径（绝对路径或 ~ 开头）"},
                "encoding": {"type": "string", "description": "文件编码，默认 utf-8"},
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "将内容写入用户本地文件。自动创建父目录。可用于导出分析报告、保存数据等。",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "文件路径"},
                "content": {"type": "string", "description": "要写入的内容"},
                "encoding": {"type": "string", "description": "文件编码，默认 utf-8"},
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "web_fetch",
        "description": "抓取指定 URL 的网页内容并返回纯文本。可用于查看财经新闻、公告、在线数据等。",
        "parameters": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "要抓取的网页 URL"},
            },
            "required": ["url"],
        },
    },
]

# 后台执行的长任务工具
BACKGROUND_TOOLS = {"screen_stocks", "generate_ai_report", "generate_strategy_decision"}

# 工具中文显示名，用于终端展示
TOOL_DISPLAY_NAMES: dict[str, str] = {
    "search_stock_by_name": "搜索股票",
    "diagnose_stock": "读盘诊断",
    "diagnose_portfolio": "持仓审判",
    "get_stock_price": "调取行情",
    "get_market_overview": "大盘水温",
    "screen_stocks": "全市场扫描",
    "generate_ai_report": "深度审讯",
    "generate_strategy_decision": "攻防决策",
    "get_recommendation_tracking": "战绩追踪",
    "get_signal_pending": "信号确认池",
    "get_portfolio": "查看持仓",
    "update_portfolio": "调仓操作",
    "check_background_tasks": "任务状态",
    "exec_command": "执行命令",
    "read_file": "读取文件",
    "write_file": "写入文件",
    "web_fetch": "抓取网页",
}


# ---------------------------------------------------------------------------
# ToolRegistry — 管理工具注册和执行
# ---------------------------------------------------------------------------

class ToolRegistry:
    """工具注册表：注册、查询 schema、执行工具。"""

    def __init__(self, user_id: str = "", access_token: str = "", refresh_token: str = ""):
        self._tool_context = ToolContext(state={
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
        })
        self._tools = self._register_tools()
        self._bg_manager = None
        self._on_bg_complete = None

    def set_background_manager(self, bg_manager, on_complete=None):
        from cli.background import BackgroundTaskManager
        self._bg_manager: BackgroundTaskManager = bg_manager
        self._on_bg_complete = on_complete

    @property
    def state(self) -> dict:
        """统一的 session state，__main__ 和工具共享同一份。"""
        return self._tool_context.state

    def _register_tools(self) -> dict[str, callable]:
        """注册所有工具函数。"""
        from agents.chat_tools import (
            search_stock_by_name,
            diagnose_stock,
            diagnose_portfolio,
            get_portfolio,
            get_stock_price,
            get_market_overview,
            screen_stocks,
            generate_ai_report,
            generate_strategy_decision,
            get_recommendation_tracking,
            get_signal_pending,
            update_portfolio,
            exec_command,
            read_file,
            write_file,
            web_fetch,
        )
        return {
            "search_stock_by_name": search_stock_by_name,
            "diagnose_stock": diagnose_stock,
            "diagnose_portfolio": diagnose_portfolio,
            "get_portfolio": get_portfolio,
            "get_stock_price": get_stock_price,
            "get_market_overview": get_market_overview,
            "screen_stocks": screen_stocks,
            "generate_ai_report": generate_ai_report,
            "generate_strategy_decision": generate_strategy_decision,
            "get_recommendation_tracking": get_recommendation_tracking,
            "get_signal_pending": get_signal_pending,
            "update_portfolio": update_portfolio,
            "exec_command": exec_command,
            "read_file": read_file,
            "write_file": write_file,
            "web_fetch": web_fetch,
        }

    def schemas(self) -> list[dict[str, Any]]:
        """返回所有工具的 JSON Schema。"""
        return TOOL_SCHEMAS

    def execute(self, name: str, args: dict[str, Any]) -> Any:
        """执行指定工具，返回结果。长任务自动提交后台。"""
        # check_background_tasks 直接返回状态
        if name == "check_background_tasks":
            if not self._bg_manager:
                return {"tasks": [], "message": "无后台任务"}
            return {"tasks": self._bg_manager.list_tasks()}

        fn = self._tools.get(name)
        if fn is None:
            return {"error": f"未知工具: {name}"}

        # 用副本注入 tool_context，避免污染原始 args（会被序列化进 messages）
        call_args = dict(args)
        sig = inspect.signature(fn)
        if "tool_context" in sig.parameters:
            call_args["tool_context"] = self._tool_context

        # 长任务提交后台
        if name in BACKGROUND_TOOLS and self._bg_manager is not None:
            task_id = f"bg_{int(time.time())}_{name}"
            display = TOOL_DISPLAY_NAMES.get(name, name)
            self._bg_manager.submit(
                task_id, name, fn, call_args,
                on_complete=self._on_bg_complete,
            )
            return {
                "status": "background",
                "task_id": task_id,
                "message": f"{display}已提交后台执行，您可以继续提问。任务完成后会自动通知。",
            }

        try:
            return fn(**call_args)
        except Exception as e:
            logger.exception("Tool %s execution failed", name)
            return {"error": f"工具执行失败: {e}"}

    def display_name(self, name: str) -> str:
        """返回工具的中文显示名。"""
        return TOOL_DISPLAY_NAMES.get(name, name)
