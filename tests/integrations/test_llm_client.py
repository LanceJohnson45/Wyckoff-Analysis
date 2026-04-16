# -*- coding: utf-8 -*-
"""integrations/llm_client.py 的 LiteLLM 开关测试。"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


class TestLiteLLMSwitch:
    """验证 LITELLM_ENABLED 环境变量路由逻辑。"""

    def test_litellm_disabled_by_default(self):
        """不设 LITELLM_ENABLED 时，不走 LiteLLM。"""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LITELLM_ENABLED", None)
            with patch("integrations.llm_client._call_gemini", return_value="native reply") as mock_native:
                from integrations.llm_client import call_llm
                result = call_llm(
                    provider="gemini",
                    model="gemini-3.1-flash-lite-preview",
                    api_key="fake-key",
                    system_prompt="test",
                    user_message="hello",
                )
                assert result == "native reply"
                mock_native.assert_called_once()

    def test_litellm_enabled_routes_to_litellm(self):
        """LITELLM_ENABLED=1 且无 images 时，走 LiteLLM。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "1"}):
            with patch(
                "integrations.llm_adapter.call_llm_via_litellm",
                return_value="litellm reply",
            ) as mock_litellm:
                from integrations.llm_client import call_llm
                result = call_llm(
                    provider="gemini",
                    model="gemini-3.1-flash-lite-preview",
                    api_key="fake-key",
                    system_prompt="test",
                    user_message="hello",
                )
                assert result == "litellm reply"
                mock_litellm.assert_called_once()

    def test_litellm_enabled_true_string(self):
        """LITELLM_ENABLED=true 也应生效。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "true"}):
            with patch(
                "integrations.llm_adapter.call_llm_via_litellm",
                return_value="litellm reply",
            ) as mock_litellm:
                from integrations.llm_client import call_llm
                result = call_llm(
                    provider="deepseek",
                    model="deepseek-chat",
                    api_key="fake-key",
                    system_prompt="test",
                    user_message="hello",
                )
                assert result == "litellm reply"

    def test_litellm_enabled_with_images_falls_back(self):
        """LITELLM_ENABLED=1 但带 images 时，降级为原生实现。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "1"}):
            with patch("integrations.llm_client._call_gemini", return_value="native with images") as mock_native:
                from integrations.llm_client import call_llm
                result = call_llm(
                    provider="gemini",
                    model="gemini-3.1-flash-lite-preview",
                    api_key="fake-key",
                    system_prompt="test",
                    user_message="hello",
                    images=[b"fake_image_bytes"],
                )
                assert result == "native with images"
                mock_native.assert_called_once()

    def test_litellm_import_error_falls_back(self):
        """LiteLLM 未安装时，降级为原生实现。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "1"}):
            with patch(
                "integrations.llm_adapter.call_llm_via_litellm",
                side_effect=ImportError("No module named 'litellm'"),
            ):
                with patch("integrations.llm_client._call_gemini", return_value="fallback reply") as mock_native:
                    from integrations.llm_client import call_llm
                    result = call_llm(
                        provider="gemini",
                        model="gemini-3.1-flash-lite-preview",
                        api_key="fake-key",
                        system_prompt="test",
                        user_message="hello",
                    )
                    assert result == "fallback reply"
                    mock_native.assert_called_once()

    def test_validation_still_works_with_litellm(self):
        """即使 LITELLM_ENABLED=1，空 api_key 仍然报错。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "1"}):
            from integrations.llm_client import call_llm
            with pytest.raises(ValueError, match="API Key 未配置"):
                call_llm(
                    provider="gemini",
                    model="test",
                    api_key="",
                    system_prompt="test",
                    user_message="hello",
                )

    def test_unsupported_provider_still_raises(self):
        """不支持的 provider 仍然报错，即使 LiteLLM 开关开启。"""
        with patch.dict(os.environ, {"LITELLM_ENABLED": "1"}):
            from integrations.llm_client import call_llm
            with pytest.raises(ValueError, match="不支持的供应商"):
                call_llm(
                    provider="unknown_provider",
                    model="test",
                    api_key="fake-key",
                    system_prompt="test",
                    user_message="hello",
                )
