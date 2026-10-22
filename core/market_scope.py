"""项目统一证券范围：A股不维护、不分析北交所及科创板。"""

from __future__ import annotations

import re

_CN_EXCLUDED_PREFIXES = ("4", "8", "92", "688", "689")


def excluded_cn_board(symbol: str, market: str = "cn") -> bool:
    """判断代码是否属于项目排除的A股板块；其他市场不受影响。"""
    if str(market).strip().lower() != "cn":
        return False
    code = str(symbol).strip().upper()
    code = re.sub(r"^(?:SH|SZ|BJ)[.]?", "", code)
    code = re.sub(r"[.](?:SH|SS|SZ|BJ|XSHG|XSHE)$", "", code)
    return bool(re.fullmatch(r"[0-9]{6}", code) and code.startswith(_CN_EXCLUDED_PREFIXES))


def require_supported_stock(symbol: str, market: str = "cn") -> None:
    """手工或缓存入口拒绝排除板块，避免绕过股票池过滤。"""
    if excluded_cn_board(symbol, market):
        raise ValueError(f"{symbol} 属于北交所或科创板，当前项目不更新、不分析该板块股票")
