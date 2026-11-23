"""行情快照、版本元数据与质量报告的不可变领域契约。"""

import re
from datetime import date
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from stock_research.identity import digest, new_id, now


class FrozenModel(BaseModel):
    """拒绝未知字段及原地属性覆盖。"""

    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


def normalize_symbol(value: str) -> str:
    """将供应商的美股后缀和类别股点号归一化。"""
    value = value.strip().upper().removeprefix("US:").removesuffix(".US").replace(".", "-")
    if not re.fullmatch(r"[A-Z][A-Z0-9]{0,9}(?:-[A-Z0-9]{1,4})?", value):
        raise ValueError("无效的美股代码")
    return value


class UniverseMember(FrozenModel):
    """一个指数或人工自选成员的来源及有效期。"""

    symbol: str
    name: str = Field(min_length=1)
    index: Literal["sp500", "nasdaq100", "manual"]
    source: str = Field(min_length=1)
    effective_date: date
    expiry_date: date | None = None
    obtained_time: AwareDatetime
    symbol_aliases: tuple[str, ...] = ()
    confirmed_suspension: str | None = None
    _normalize = field_validator("symbol")(normalize_symbol)

    @model_validator(mode="after")
    def validate_period(self) -> "UniverseMember":
        """拒绝倒置有效期及空停牌证据。"""
        if self.expiry_date is not None and self.expiry_date <= self.effective_date:
            raise ValueError("成员失效日期须晚于生效日期")
        if self.confirmed_suspension is not None and not self.confirmed_suspension.strip():
            raise ValueError("停牌必须有来源证据")
        return self


class UniverseVersion(FrozenModel):
    """指数并集版本，保留同股的多重成员关系。"""

    version: str = Field(min_length=1)
    create_time: AwareDatetime = Field(default_factory=now)
    members: tuple[UniverseMember, ...]
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_members(self) -> "UniverseVersion":
        """每一指数成员有效期只保存一份。"""
        keys = [(m.index, m.symbol, m.effective_date) for m in self.members]
        if len(set(keys)) != len(keys):
            raise ValueError("股票池存在重复成员")
        return self

    def active(self, as_of: date) -> dict[str, tuple[UniverseMember, ...]]:
        """按生效区间取并集，允许显式标注的当前池历史研究。"""
        result: dict[str, tuple[UniverseMember, ...]] = {}
        for member in self.members:
            if member.effective_date <= as_of and (member.expiry_date is None or as_of < member.expiry_date):
                result[member.symbol] = (*result.get(member.symbol, ()), member)
        return dict(sorted(result.items()))


class Classification(FrozenModel):
    """已取得的公司行业及人工确认主题。"""

    symbol: str
    company_name: str = Field(min_length=1)
    industry: str | None
    themes: tuple[str, ...] = ()
    source: str = Field(min_length=1)
    obtained_time: AwareDatetime
    effective_date: date
    _normalize = field_validator("symbol")(normalize_symbol)


class ClassificationVersion(FrozenModel):
    """报告引用的分类版本；缺历史资料时保留限制。"""

    version: str = Field(min_length=1)
    records: tuple[Classification, ...]
    limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_symbols(self) -> "ClassificationVersion":
        """拒绝同一版本内有歧义的重复分类。"""
        if len({r.symbol for r in self.records}) != len(self.records):
            raise ValueError("分类版本内股票重复")
        return self


class Bar(FrozenModel):
    """带来源和修订的完整日线，session_date 始终是美国交易日期。"""

    symbol: str
    market: Literal["us"] = "us"
    kind: Literal["stock", "benchmark"]
    session_date: date
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)
    adjustment: Literal["qfq"] = "qfq"
    source: str
    source_revision: AwareDatetime
    observed_time: AwareDatetime

    @model_validator(mode="after")
    def validate_prices(self) -> "Bar":
        """拒绝 OHLC 关系损坏，不修补或伪造数据。"""
        if self.low > min(self.open, self.close) or self.high < max(self.open, self.close) or self.low > self.high:
            raise ValueError("OHLC 关系不合理")
        return self


class QualityIssue(FrozenModel):
    """可解释的排除原因，策略历史长度不足分别记录。"""

    symbol: str
    kind: Literal["stock", "benchmark"]
    reason: str
    strategy: Literal["all", "martin", "wyckoff"] = "all"


class QualityReport(FrozenModel):
    """整体日期覆盖与按策略可用范围。"""

    passed: bool
    expected_session: date
    universe_count: int
    suspended_count: int
    aligned_count: int
    coverage: float
    threshold: float
    eligible: dict[str, tuple[str, ...]]
    issues: tuple[QualityIssue, ...]
    limitations: tuple[str, ...]


class Snapshot(FrozenModel):
    """不可变物化行情及版本全文，按内容校验防止历史快照被覆盖。"""

    id: str = Field(default_factory=new_id)
    create_time: AwareDatetime = Field(default_factory=now)
    as_of: date
    universe: UniverseVersion
    classifications: ClassificationVersion
    bars: tuple[Bar, ...]
    quality: QualityReport
    source_identity: str
    content_hash: str

    def content(self) -> dict[str, object]:
        """摘要排除生成时间和随机标识；涵盖数据及完整版本内容。"""
        return self.model_dump(mode="json", exclude={"id", "create_time", "content_hash"})

    @model_validator(mode="after")
    def validate_content(self) -> "Snapshot":
        """读取和写入均拒绝摘要不符或含未来行情的快照。"""
        if self.content_hash != digest(self.content()):
            raise ValueError("快照内容摘要不匹配")
        if any(bar.session_date > self.as_of for bar in self.bars):
            raise ValueError("快照包含截止日之后的行情")
        keys = [(b.kind, b.symbol, b.session_date) for b in self.bars]
        if len(set(keys)) != len(keys):
            raise ValueError("快照存在重复日线")
        return self
