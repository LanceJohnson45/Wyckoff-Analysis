# -*- coding: utf-8 -*-
"""LLM Provider 抽象接口 — 所有模型供应商实现这个接口。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class LLMProvider(ABC):
    """
    统一 LLM 调用接口。

    每个 provider 把各自 SDK 的响应翻译成统一格式：
    - {"type": "text", "text": "..."}
    - {"type": "tool_calls", "tool_calls": [{"id", "name", "args"}]}
    """

    @abstractmethod
    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system_prompt: str = "",
    ) -> dict[str, Any]:
        """
        发送对话消息，返回模型响应。

        Parameters
        ----------
        messages : 对话历史，格式：
            [{"role": "user"|"assistant"|"tool", "content": "...", ...}]
        tools : 工具 JSON Schema 列表
        system_prompt : 系统提示词

        Returns
        -------
        {"type": "text", "text": "最终回答"}
        或
        {"type": "tool_calls", "tool_calls": [{"id": "...", "name": "...", "args": {...}}]}
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider 显示名称。"""
        ...
