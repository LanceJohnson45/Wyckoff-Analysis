"""内容身份必须与既有规范JSON字节保持完全一致。"""

import hashlib

import pytest

from stock_research.identity import canonical_json, digest


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"z": "中文😀", "a": [1, 1.0, True, False, None, {"β": '"\\\n'}]},
        {"bars": [{"symbol": "AAPL", "close": 1.2}] * 100, "quality": "缺行业"},
        ["中文", {"z": 1, "a": 2}],
        {1: "integer-key"},
    ],
    ids=["empty", "unicode-nested-scalars", "large-ascii-with-unicode-metadata", "list", "integer-key"],
)
def test_unit_digest_preserves_canonical_json_identity(value: object) -> None:
    """[UNIT] 大字段分段摘要仍使用原字节协议，不改变历史身份。"""
    expected = hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()

    assert digest(value) == expected


def test_unit_digest_rejects_nonfinite_nested_values() -> None:
    """[UNIT] 分段优化不能把非有限行情写成合法身份。"""
    with pytest.raises(ValueError):
        digest({"bars": [float("nan")]})
