# -*- coding: utf-8 -*-
"""OpenAI Provider — openai SDK 实现。"""
from __future__ import annotations

import json
from typing import Any

import openai

from cli.providers.base import LLMProvider


class OpenAIProvider(LLMProvider):
    """通过 openai SDK 调用 OpenAI 模型。"""

    def __init__(self, api_key: str, model: str = "gpt-4o", base_url: str = ""):
        kwargs: dict[str, Any] = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url.rstrip("/")
        self._client = openai.OpenAI(**kwargs)
        self._model = model

    @property
    def name(self) -> str:
        return f"OpenAI ({self._model})"

    def chat(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        system_prompt: str = "",
    ) -> dict[str, Any]:
        # 构建 OpenAI messages
        oai_messages = self._build_messages(messages, system_prompt)

        # 构建工具声明
        oai_tools = self._build_tools(tools) if tools else None

        kwargs: dict[str, Any] = {
            "model": self._model,
            "messages": oai_messages,
        }
        if oai_tools:
            kwargs["tools"] = oai_tools

        response = self._client.chat.completions.create(**kwargs)
        return self._parse_response(response)

    def _build_messages(self, messages: list[dict], system_prompt: str) -> list[dict]:
        """将统一消息格式转为 OpenAI messages 格式。"""
        oai_msgs = []
        if system_prompt:
            oai_msgs.append({"role": "system", "content": system_prompt})

        for msg in messages:
            role = msg["role"]

            if role == "user":
                oai_msgs.append({"role": "user", "content": msg["content"]})

            elif role == "assistant":
                oai_msg: dict[str, Any] = {"role": "assistant"}
                if msg.get("content"):
                    oai_msg["content"] = msg["content"]
                if msg.get("tool_calls"):
                    oai_msg["tool_calls"] = [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["args"], ensure_ascii=False),
                            },
                        }
                        for tc in msg["tool_calls"]
                    ]
                oai_msgs.append(oai_msg)

            elif role == "tool":
                result = msg["content"]
                if not isinstance(result, str):
                    result = json.dumps(result, ensure_ascii=False)
                oai_msgs.append({
                    "role": "tool",
                    "tool_call_id": msg.get("tool_call_id", ""),
                    "content": result,
                })

        return oai_msgs

    def _build_tools(self, tools: list[dict]) -> list[dict]:
        """将标准 function schema 转为 OpenAI tools 格式。"""
        return [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("parameters", {"type": "object", "properties": {}}),
                },
            }
            for t in tools
        ]

    def _parse_response(self, response) -> dict[str, Any]:
        """解析 OpenAI 响应为统一格式。"""
        choice = response.choices[0]
        message = choice.message

        if message.tool_calls:
            tool_calls = []
            for tc in message.tool_calls:
                try:
                    args = json.loads(tc.function.arguments)
                except (json.JSONDecodeError, TypeError):
                    args = {}
                tool_calls.append({
                    "id": tc.id,
                    "name": tc.function.name,
                    "args": args,
                })
            return {
                "type": "tool_calls",
                "tool_calls": tool_calls,
                "text": message.content or "",
            }

        return {"type": "text", "text": message.content or ""}
