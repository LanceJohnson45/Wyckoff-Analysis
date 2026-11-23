"""研究领域统一的操作时间、实体标识和内容摘要。"""

import hashlib
import json
from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


def now() -> datetime:
    """生成带东八区时区的操作时间。"""
    return datetime.now(SHANGHAI)


def new_id() -> str:
    """生成对外使用的随机 UUID4。"""
    return str(uuid4())


def canonical_json(value: object) -> str:
    """拒绝非有限数值，生成稳定、可校验的 JSON。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: object) -> str:
    """计算规范化 JSON 的 SHA256。"""
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()
