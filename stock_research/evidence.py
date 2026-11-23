"""每阶段固定的只读证据、历史及结构化数值验证。"""

from datetime import date, datetime
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from stock_research.analysis import MartinReport
from stock_research.identity import digest
from stock_research.models import FrozenModel, Snapshot
from stock_research.wyckoff import WyckoffReport, json_values


class Evidence(FrozenModel):
    """事实引用只能来自固定产物，观点文本不作为可验证事实。"""

    ref: str
    symbol: str | None
    value: str | float | bool | None


class EvidenceContext(FrozenModel):
    """绑定单workspace/run/stage的授权，不允许调用者切换数据源。"""

    workspace: str
    run_id: str
    stage_id: str
    strategy: Literal["martin", "wyckoff"]
    role: Literal["select", "review"]
    snapshot_id: str
    snapshot_hash: str
    analysis_hash: str
    as_of: date
    deadline: AwareDatetime
    tool_budget: int = Field(default=20, ge=1, le=20)
    pool: tuple[str, ...]
    symbols: tuple[str, ...]
    facts: dict[str, Evidence]
    histories: dict[str, tuple[dict[str, object], ...]]
    limitations: tuple[str, ...]
    content_hash: str

    @model_validator(mode="after")
    def verify_content(self) -> "EvidenceContext":
        """历史和引用均在启动时验证，无法通过文件替换混入另一阶段。"""
        if digest(self.model_dump(mode="json", exclude={"content_hash"})) != self.content_hash:
            raise ValueError("阶段证据摘要不匹配")
        if not set(self.pool) <= set(self.symbols) or any(key != item.ref for key, item in self.facts.items()):
            raise ValueError("阶段证据范围无效")
        if any(item.symbol is not None and item.symbol not in self.symbols for item in self.facts.values()):
            raise ValueError("阶段包含未授权证券的事实")
        for key, rows in self.histories.items():
            kind, symbol = key.split(":", 1)
            if kind not in {"stock", "benchmark"} or (
                symbol not in self.symbols if kind == "stock" else symbol not in {"SPY", "QQQ", "IWM"}
            ):
                raise ValueError("历史证券超出阶段授权")
            dates = [str(row["date"]) for row in rows]
            if dates != sorted(set(dates)) or any(day > self.as_of.isoformat() for day in dates):
                raise ValueError("历史日期重复、倒序或超过截止日")
        return self

    def resolve(self, ref: str) -> Evidence:
        """只解析已授权事实或固定历史字段，引用不是文件路径。"""
        if ref in self.facts:
            return self.facts[ref]
        pieces = ref.split("/")
        if len(pieces) == 5 and pieces[0] == "bar" and pieces[4] in {"open", "high", "low", "close", "volume"}:
            _, kind, symbol, day, field = pieces
            for row in self.histories.get(f"{kind}:{symbol}", ()):
                if row["date"] == day:
                    return Evidence(ref=ref, symbol=symbol if kind == "stock" else None, value=row[field])
        raise ValueError("证据引用不在当前阶段范围内")


def flatten_facts(value: object, prefix: str, symbol: str | None, result: dict[str, Evidence]) -> None:
    """仅将标量注册为可核验值，嵌套路径用于定位原始因子与条件。"""
    if isinstance(value, dict):
        for key, child in value.items():
            flatten_facts(child, f"{prefix}/{key}", symbol, result)
    elif isinstance(value, (tuple, list)):
        for index, child in enumerate(value):
            flatten_facts(child, f"{prefix}/{index}", symbol, result)
    elif value is None or isinstance(value, (str, float, int, bool)):
        result[prefix] = Evidence(ref=prefix, symbol=symbol, value=value)
    else:
        raise ValueError("证据包含不支持的非标量")


def build_context(
    snapshot: Snapshot,
    analysis: MartinReport | WyckoffReport,
    *,
    workspace: str,
    run_id: str,
    stage_id: str,
    role: Literal["select", "review"],
    deadline: datetime,
    targets: tuple[str, ...] = (),
) -> EvidenceContext:
    """初选仅见自身池；互评只开放目标股票的本策略事实和共同历史。"""
    if analysis.snapshot_hash != snapshot.content_hash or not snapshot.quality.passed:
        raise ValueError("策略产物与合格快照不一致")
    strategy = "martin" if isinstance(analysis, MartinReport) else "wyckoff"
    symbols = analysis.pool if role == "select" else targets
    facts = {}
    flatten_facts({"pool_size": len(analysis.pool), "strategy": strategy}, "scope", None, facts)
    flatten_facts(analysis.limitations, "limitations", None, facts)
    calculated = set()
    for observation in analysis.observations:
        row = observation.model_dump(mode="json") if isinstance(analysis, MartinReport) else observation
        if row["symbol"] in symbols:
            calculated.add(row["symbol"])
            flatten_facts(row, f"{strategy}/{row['symbol']}", row["symbol"], facts)
    for symbol in symbols:
        flatten_facts({"calculation_available": symbol in calculated}, f"{strategy}/{symbol}", symbol, facts)
    if isinstance(analysis, WyckoffReport):
        flatten_facts(analysis.metrics["benchmark"], "market/SPY", None, facts)
        for row in analysis.candidates:
            if row["symbol"] in symbols:
                flatten_facts(row, f"wyckoff/{row['symbol']}/candidate", row["symbol"], facts)
    else:
        for group in analysis.groups:
            if set(group["members"]) & set(symbols):
                flatten_facts(group, "group/" + digest(group["name"])[:16], None, facts)
    histories = {}
    for bar in snapshot.bars:
        if (bar.kind == "stock" and bar.symbol in symbols) or bar.kind == "benchmark":
            histories.setdefault(f"{bar.kind}:{bar.symbol}", []).append(
                {
                    "date": bar.session_date.isoformat(),
                    **{key: getattr(bar, key) for key in ("open", "high", "low", "close", "volume")},
                }
            )
    body = json_values(
        dict(
            workspace=workspace,
            run_id=run_id,
            stage_id=stage_id,
            strategy=strategy,
            role=role,
            snapshot_id=snapshot.id,
            snapshot_hash=snapshot.content_hash,
            analysis_hash=analysis.content_hash,
            as_of=snapshot.as_of,
            deadline=deadline,
            tool_budget=20,
            pool=analysis.pool if role == "select" else (),
            symbols=symbols,
            facts={key: value.model_dump(mode="json") for key, value in facts.items()},
            histories=histories,
            limitations=analysis.limitations,
        )
    )
    return EvidenceContext(**body, content_hash=digest(body))
