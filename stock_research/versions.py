"""有出处的成分与分类版本导入，不自动推断历史成分。"""

import json
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from stock_research.artifacts import write_once
from stock_research.identity import digest, now
from stock_research.models import Classification, ClassificationVersion, UniverseMember, UniverseVersion


def validate_universe(version: UniverseVersion, as_of: date) -> None:
    """完整两指数数量仅作截断保护，不作为来源真实性证明。"""
    active = version.active(as_of)
    counts = {
        index: sum(any(m.index == index for m in memberships) for memberships in active.values())
        for index in ("sp500", "nasdaq100")
    }
    if not 490 <= counts["sp500"] <= 530 or not 95 <= counts["nasdaq100"] <= 110:
        raise ValueError("指数成分数量异常：须提供完整标普500及纳斯达克100，不能用旧520只列表替代")


def import_version(kind: Literal["universe", "classifications"], source: Path, destination: Path) -> str:
    """验证完整版本后原子导入；指数成员数量只作明显截断保护。"""
    model = UniverseVersion if kind == "universe" else ClassificationVersion
    version = model.model_validate_json(source.read_text(encoding="utf-8"))
    if isinstance(version, UniverseVersion):
        validate_universe(version, max((m.effective_date for m in version.members), default=version.create_time.date()))
    elif not version.records:
        raise ValueError("分类版本不能为空")
    write_once(destination, version.model_dump_json(indent=2) + "\n")
    return version.version


def build_universe(
    sp500: Path,
    nasdaq100: Path,
    *,
    version: str,
    effective_date: date,
    source_reference: str,
    manual: Path | None = None,
) -> UniverseVersion:
    """从分开的带公司名 JSON 清单构建并集，保留源内容摘要及原始代码。"""
    if not source_reference.strip():
        raise ValueError("成分来源引用不能为空")
    members = []
    for index, path in (("sp500", sp500), ("nasdaq100", nasdaq100), ("manual", manual)):
        if path is None:
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError("成分文件必须是包含 symbol/name 的 JSON 数组")
        if any(not isinstance(row, dict) or not row.get("symbol") or not row.get("name") for row in payload):
            raise ValueError("成分行必须包含非空 symbol/name")
        source = f"{source_reference}:{index}:sha256:{digest(payload)}"
        members.extend(
            UniverseMember(
                symbol=row["symbol"],
                name=row["name"],
                index=index,
                source=source,
                symbol_aliases=(row["symbol"],),
                effective_date=effective_date,
                obtained_time=now(),
            )
            for row in payload
        )
    result = UniverseVersion(
        version=version,
        members=tuple(members),
        limitations=("输入清单只代表本次提供的成员集合；数量校验不能证明实时成分或严格历史时点，须独立核验来源",),
    )
    validate_universe(result, effective_date)
    return result


def build_classifications(
    cache: Path, universe: UniverseVersion, *, version: str, effective_date: date, source_reference: str
) -> ClassificationVersion:
    """迁移原有 yfinance 缓存；缺行业保留为空，不自动生成或确认主题。"""
    payload = json.loads(cache.read_text(encoding="utf-8"))
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, dict):
        raise ValueError("分类缓存缺少 items 映射")
    source = f"{source_reference}:sha256:{digest(payload)}"
    records = []
    for symbol, memberships in universe.active(effective_date).items():
        record = items.get(symbol) or items.get(symbol.replace("-", "."))
        if record is None:
            continue
        if not isinstance(record, dict):
            raise ValueError("分类缓存行必须是对象")
        if record.get("market") != "us":
            continue
        timestamp = record.get("updated_at") or payload.get("updated_at")
        if not timestamp:
            raise ValueError("分类缓存缺少实际取得时间，不能用导入时间冒充")
        records.append(
            Classification(
                symbol=symbol,
                company_name=memberships[0].name,
                industry=(record.get("industry") or "").strip() or None,
                source=source,
                effective_date=effective_date,
                obtained_time=datetime.fromisoformat(timestamp),
            )
        )
    return ClassificationVersion(
        version=version,
        records=tuple(records),
        limitations=("来自已有分类缓存，未经在线刷新；导入生效日不代表行业实际变更日，主题尚未人工分配",),
    )
