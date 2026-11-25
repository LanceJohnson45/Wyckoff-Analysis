"""首轮单组参数实验的受控变体。"""

from typing import Literal

from pydantic import BaseModel

from stock_research.parameters import MartinParameters

ExperimentField = Literal["support_atr", "extension_atr", "contraction_ratio"]
_FIELDS: tuple[ExperimentField, ...] = ("support_atr", "extension_atr", "contraction_ratio")


class ParameterExperiment(BaseModel):
    """实验值必须随报告冻结，不冒充生产发布。"""

    field: ExperimentField
    value: float
    parameters: MartinParameters


class CombinedParameterExperiment(BaseModel):
    """受控组合只使用已完成单组中的值，不冒充生产发布。"""

    fields: tuple[ExperimentField, ExperimentField, ExperimentField]
    values: tuple[float, float, float]
    parameters: MartinParameters


_COMBINED_FIELDS: tuple[ExperimentField, ExperimentField, ExperimentField] = (
    "support_atr",
    "extension_atr",
    "contraction_ratio",
)
_COMBINED_VERSION_PREFIX = "experiment:combined"


def experiment_parameter_sets(
    baseline: MartinParameters, field: str, values: tuple[float, float, float]
) -> tuple[ParameterExperiment, ...]:
    """只开放计划首轮三个字段，其他字段必须另立算法/实验设计。"""
    if field not in _FIELDS:
        raise ValueError("实验字段不在首轮单组参数清单")
    if len(set(values)) != 3:
        raise ValueError("实验字段必须提供三个不重复值")
    return tuple(
        ParameterExperiment(
            field=field,
            value=value,
            parameters=baseline.model_copy(
                update={field: value, "version": f"{baseline.version}:experiment:{field}:{value}"}
            ),
        )
        for value in values
    )


def combined_parameter_experiment(
    baseline: MartinParameters,
    fields: tuple[str, str, str],
    values: tuple[float, float, float],
) -> CombinedParameterExperiment:
    """有限组合必须覆盖三个计划字段且值来自显式输入，不接受任意参数代码。"""
    if len(fields) != 3 or set(fields) != set(_FIELDS):
        raise ValueError("组合实验必须且只能包含三个计划字段")
    defaults = {
        "support_atr": baseline.support_atr,
        "extension_atr": baseline.extension_atr,
        "contraction_ratio": baseline.contraction_ratio,
    }
    if len(set(values)) != 3 or any(defaults[field] == value for field, value in zip(fields, values, strict=True)):
        raise ValueError("组合实验必须提供三个不重复且非生产默认值")
    updates = dict(zip(fields, values, strict=True))
    if updates["support_atr"] >= updates["extension_atr"]:
        raise ValueError("组合实验观察带宽须小于延伸距离")
    version = f"{baseline.version}:{_COMBINED_VERSION_PREFIX}:{updates['support_atr']}:{updates['extension_atr']}:{updates['contraction_ratio']}"
    return CombinedParameterExperiment(
        fields=_COMBINED_FIELDS,
        values=(updates["support_atr"], updates["extension_atr"], updates["contraction_ratio"]),
        parameters=baseline.model_copy(update={**updates, "version": version}),
    )
