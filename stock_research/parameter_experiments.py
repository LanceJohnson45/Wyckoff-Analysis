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
