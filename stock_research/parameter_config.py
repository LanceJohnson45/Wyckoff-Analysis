"""解析并物化一次参数配置，后续量化和模型阶段只消费冻结值。"""

import asyncio

from stock_research.artifacts import write_once
from stock_research.config import Settings
from stock_research.errors import ResearchIntegrityError, ResearchUnavailable
from stock_research.identity import canonical_json, digest
from stock_research.parameter_store import published_parameters
from stock_research.parameters import MartinParameters
from stock_research.run_store import RunStore
from stock_research.schema import RESEARCH_RUNS
from stock_research.schemas.parameters import StrategyParameterList, StrategyParameterView
from stock_research.wyckoff import WyckoffParameters


async def current_parameters(store: RunStore, settings: Settings) -> StrategyParameterList:
    """数据库没有发布记录时使用既有operator基线；存储错误不回退。"""
    published = await published_parameters(store)
    for strategy, model, path in (
        ("martin", MartinParameters, settings.parameters_path),
        ("wyckoff", WyckoffParameters, settings.wyckoff_parameters_path),
    ):
        if strategy in published:
            continue
        try:
            value = await asyncio.to_thread(
                lambda model=model, path=path: model.model_validate_json(path.read_text(), strict=True)
            )
        except (OSError, ValueError):
            raise ResearchUnavailable("研究参数基线不可用，请检查operator版本文件") from None
        published[strategy] = StrategyParameterView(
            strategy=strategy,
            parameters=value,
            source="operator_baseline",
            revision=0,
            parameter_set_id=None,
            parameter_hash=digest(value.model_dump(mode="json")),
        )
    return StrategyParameterList(items=tuple(published[key] for key in ("martin", "wyckoff")))


async def resolve_run_parameters(store: RunStore, settings: Settings) -> tuple[Settings, dict[str, object]]:
    """两策略先统一解析，物化路径只由参数摘要生成，不暴露给HTTP调用者。"""
    current = await current_parameters(store, settings)
    paths, sources = {}, {}
    for entry in current.items:
        data = entry.parameters.model_dump(mode="json")
        path = settings.artifacts_path / "parameters" / f"{entry.strategy}-{entry.parameter_hash}.json"
        await asyncio.to_thread(write_once, path, canonical_json(data) + "\n")
        paths["parameters_path" if entry.strategy == "martin" else "wyckoff_parameters_path"] = path
        sources[entry.strategy] = entry.model_dump(mode="json", exclude={"parameters", "strategy"})
    return settings.model_copy(update=paths), sources


async def frozen_parameters(store: RunStore, run_id: str) -> StrategyParameterList:
    """固定运行仅取其清单，不从当前发布状态改写历史参数。"""
    manifest = (await store.row(RESEARCH_RUNS, run_id))["payload"]
    items = []
    for strategy in ("martin", "wyckoff"):
        parameters = manifest["parameters"][strategy]
        source = manifest.get("parameter_sources", {}).get(
            strategy,
            {
                "source": "operator_baseline",
                "revision": 0,
                "parameter_set_id": None,
                "parameter_hash": digest(parameters),
            },
        )
        try:
            view = StrategyParameterView(strategy=strategy, parameters=parameters, **source)
        except ValueError:
            raise ResearchIntegrityError("冻结参数版本无效") from None
        if view.parameter_hash != digest(view.parameters.model_dump(mode="json")):
            raise ResearchIntegrityError("冻结参数摘要不匹配")
        items.append(view)
    return StrategyParameterList(items=tuple(items))
