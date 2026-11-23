"""版本参数请求与公开视图；参数按指定策略严格解析。"""

from typing import Literal

from pydantic import UUID4, AwareDatetime, Field, model_validator

from stock_research.identity import canonical_json
from stock_research.parameters import MartinParameters
from stock_research.schemas.api import ApiModel, Strategy
from stock_research.wyckoff import WyckoffParameters

ParameterValues = MartinParameters | WyckoffParameters


class CreateParameterSet(ApiModel):
    strategy: Strategy
    parameters: ParameterValues

    @model_validator(mode="before")
    @classmethod
    def strict_strategy_parameters(cls, value: object) -> object:
        """JSON数组可表达tuple，数字字符串或错误策略字段不能被隐式修补。"""
        if not isinstance(value, dict) or value.get("strategy") not in {"martin", "wyckoff"}:
            return value
        model = MartinParameters if value["strategy"] == "martin" else WyckoffParameters
        parameters = value.get("parameters")
        if isinstance(parameters, (MartinParameters, WyckoffParameters)):
            parameters = parameters.model_dump(mode="json")
        return {**value, "parameters": model.model_validate_json(canonical_json(parameters), strict=True)}


class ParameterSetView(CreateParameterSet):
    id: UUID4
    parameter_hash: str
    create_time: AwareDatetime


class ParameterSetPage(ApiModel):
    items: tuple[ParameterSetView, ...]
    total: int
    page: int
    page_size: int


class ActivateParameters(ApiModel):
    parameter_set_id: UUID4
    expected_revision: int = Field(ge=0, strict=True)


class StrategyParameterView(CreateParameterSet):
    source: Literal["operator_baseline", "published"]
    revision: int
    parameter_set_id: UUID4 | None
    parameter_hash: str


class StrategyParameterList(ApiModel):
    items: tuple[StrategyParameterView, ...]
