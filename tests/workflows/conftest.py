"""工作流测试只使用各用例声明的漏斗配置，隔离其他套件加载的本机.env。"""

import os

import pytest


@pytest.fixture(autouse=True)
def isolated_market_funnel_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """环境覆盖是测试输入；未声明的本机参数不能污染生产/回测对拍。"""
    for key in tuple(os.environ):
        if key.startswith(("MARKET_FUNNEL_", "FUNNEL_CFG_")):
            monkeypatch.delenv(key)
