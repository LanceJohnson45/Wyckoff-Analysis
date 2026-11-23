"""人工主题提议/确认和自选追加修订的显式契约。"""

from datetime import date
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import UUID4, AwareDatetime, Field, StringConstraints, field_validator, model_validator

from stock_research.models import normalize_symbol
from stock_research.schemas.api import ApiModel

Text = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=512)]


class ThemeDefinition(ApiModel):
    name: Text = Field(max_length=128)
    members: tuple[str, ...] = Field(max_length=2000)
    source: Text
    reason: str = Field(min_length=1, max_length=3000)

    @field_validator("name", "source", "reason")
    @classmethod
    def meaningful_text(cls, value: str) -> str:
        """自由文本只作资料，拒绝数据库不接受的NUL和损坏Unicode。"""
        value = value.strip()
        if not value or "\x00" in value:
            raise ValueError("资料不能为空或包含NUL")
        value.encode("utf-8")
        return value

    @field_validator("members")
    @classmethod
    def canonical_members(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        """规范化类别股后仍必须唯一，不隐式吞掉重复输入。"""
        result = tuple(normalize_symbol(value) for value in values)
        if len(set(result)) != len(result):
            raise ValueError("主题成员重复")
        return tuple(sorted(result))


class ConfirmTheme(ApiModel):
    expected_revision: int = Field(ge=0)
    effective_date: date


class ManualDefinition(ApiModel):
    symbol: str
    action: Literal["add", "remove"]
    name: Text | None = None
    industry: Text | None = None
    source: Text
    reason: str = Field(min_length=1, max_length=3000)
    _symbol = field_validator("symbol")(normalize_symbol)
    _text = field_validator("name", "industry", "source", "reason")(
        lambda value: ThemeDefinition.meaningful_text(value) if value is not None else None
    )

    @model_validator(mode="after")
    def meaningful_action(self) -> "ManualDefinition":
        """移除只针对manual关系，不混入未被使用的公司资料变更。"""
        if self.action == "add" and self.name is None:
            raise ValueError("加入自选须提供公司名称")
        if self.action == "remove" and (self.name is not None or self.industry is not None):
            raise ValueError("移除自选不接受公司资料")
        return self


class CreateManualOverride(ManualDefinition):
    expected_revision: int = Field(ge=0)
    effective_date: date


class InputPublication(ApiModel):
    id: UUID4
    kind: Literal["theme", "manual"]
    target: Text
    revision: int = Field(ge=1)
    previous_hash: str | None
    proposal_id: UUID4 | None
    effective_date: date
    confirmed_time: AwareDatetime
    definition: ThemeDefinition | ManualDefinition
    content_hash: str

    @model_validator(mode="after")
    def consistent_definition(self) -> "InputPublication":
        """发布类型、对象键和提议外键不能与合法内容交叉拼接。"""
        theme = isinstance(self.definition, ThemeDefinition)
        if (self.kind == "theme") != theme or theme != bool(self.proposal_id):
            raise ValueError("发布类型或提议关联不一致")
        key = self.definition.name if theme else self.definition.symbol
        if self.target != key or (self.revision == 1) != (self.previous_hash is None):
            raise ValueError("发布目标或前序修订不一致")
        if self.effective_date < self.confirmed_time.astimezone(ZoneInfo("America/New_York")).date():
            raise ValueError("人工发布不能追溯生效")
        return self


class ThemeProposal(ApiModel):
    id: UUID4
    definition: ThemeDefinition
    base_revision: int = Field(ge=0)
    base_universe_hash: str
    base_classification_hash: str
    previous_members: tuple[str, ...]
    current_revision: int = Field(ge=0)
    status: Literal["pending", "confirmed"]
    publication: InputPublication | None
    create_time: AwareDatetime

    @model_validator(mode="after")
    def consistent_status(self) -> "ThemeProposal":
        """未确认与确认必须由真实发布关联区分。"""
        if (self.status == "confirmed") != (self.publication is not None):
            raise ValueError("提议状态与发布不一致")
        if self.publication and (
            self.publication.proposal_id != self.id or self.publication.definition != self.definition
        ):
            raise ValueError("确认版本不属于该提议")
        return self


class ThemeProposalPage(ApiModel):
    items: tuple[ThemeProposal, ...]
    total: int
    page: int
    page_size: int


class InputPublicationPage(ApiModel):
    items: tuple[InputPublication, ...]
    total: int
    page: int
    page_size: int


class ManualHead(ApiModel):
    symbol: str
    revision: int = Field(ge=0)
    publication: InputPublication | None


class InputSources(ApiModel):
    source: Literal["operator_baseline", "published", "frozen_snapshot"]
    as_of: date
    universe_version: str
    classification_version: str
    universe_hash: str
    classification_hash: str
    base_universe_hash: str | None
    base_classification_hash: str | None
    publications: tuple[InputPublication, ...]

    @model_validator(mode="after")
    def consistent_sources(self) -> "InputSources":
        """冻结来源不能用未来发布或当前空配置冒充已消费的确认版本。"""
        if (self.source == "published") != bool(self.publications) or any(
            row.effective_date > self.as_of for row in self.publications
        ):
            raise ValueError("冻结来源与已生效发布不一致")
        return self


class FrozenInputs(ApiModel):
    run_id: UUID4
    inputs: InputSources
