# -*- coding: utf-8 -*-
"""
Agent 核心循环 — 整个 CLI 最核心的文件。

原理极简：
    while True:
        response = llm.chat(messages, tools)
        if 是文本 → 返回给用户
        if 是工具调用 → 执行工具 → 把结果塞回 messages → 继续循环
"""
from __future__ import annotations

import json
import logging
from typing import Any

from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

from cli.providers.base import LLMProvider
from cli.tools import ToolRegistry

logger = logging.getLogger(__name__)

_THINKING_TEXT = Text.from_markup("  [dim]思考中…[/dim]")

# 单轮最大工具调用次数（防止死循环）
MAX_TOOL_ROUNDS = 15


def run(
    provider: LLMProvider,
    tools: ToolRegistry,
    messages: list[dict[str, Any]],
    system_prompt: str = "",
    on_tool_call: callable = None,
    on_tool_result: callable = None,
    console=None,
) -> str:
    """
    执行一次完整的 Agent 循环，返回最终文本回答。

    Parameters
    ----------
    provider : LLM 供应商实例
    tools : 工具注册表
    messages : 对话历史（会被原地修改，追加 assistant 和 tool 消息）
    system_prompt : 系统提示词
    on_tool_call : 回调函数 (name, args) → 工具调用时通知 UI
    on_tool_result : 回调函数 (name, result) → 工具返回时通知 UI

    Returns
    -------
    模型最终的文本回答
    """
    for round_idx in range(MAX_TOOL_ROUNDS):
        if console:
            live = Live(Spinner("dots", text=_THINKING_TEXT), console=console, refresh_per_second=10, transient=True)
            live.start()
        else:
            live = None
        try:
            response = provider.chat(messages, tools.schemas(), system_prompt)
        finally:
            if live:
                live.stop()

        if response["type"] == "text":
            # 模型给出了最终文本回答
            text = response.get("text", "")
            messages.append({"role": "assistant", "content": text})
            return text

        if response["type"] == "tool_calls":
            tool_calls = response["tool_calls"]

            # 如果模型同时返回了文本（Claude 会这样做）
            partial_text = response.get("text", "")

            # 记录 assistant 消息（包含工具调用）
            assistant_msg: dict[str, Any] = {"role": "assistant", "tool_calls": tool_calls}
            if partial_text:
                assistant_msg["content"] = partial_text
            messages.append(assistant_msg)

            # 逐个执行工具
            for call in tool_calls:
                name = call["name"]
                args = call["args"]
                call_id = call["id"]

                if on_tool_call:
                    on_tool_call(name, args)

                result = tools.execute(name, args)

                if on_tool_result:
                    on_tool_result(name, result)

                # 工具结果追加到 messages
                messages.append({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "name": name,
                    "content": json.dumps(result, ensure_ascii=False, default=str),
                })

    # 超过最大轮次
    return "(Agent 工具调用轮次超限，已停止)"
