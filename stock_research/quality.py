"""数据阶段质量门槛和不可变快照构建。"""

from collections import Counter, defaultdict
from datetime import date

from stock_research.calendar import sessions_ending
from stock_research.identity import digest
from stock_research.models import Bar, ClassificationVersion, QualityIssue, QualityReport, Snapshot, UniverseVersion

BENCHMARKS = ("SPY", "QQQ", "IWM")
HISTORY_REQUIREMENTS = {"martin": 100, "wyckoff": 320}


def build_snapshot(
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    bars: tuple[Bar, ...],
    source_identity: str,
    as_of: date,
    *,
    coverage_min: float = 0.95,
    history_rows: int = 320,
    source_issues: tuple[QualityIssue, ...] = (),
    extra_limitations: tuple[str, ...] = (),
) -> Snapshot:
    """冻结共同窗口、质量策略和版本全文；同日股票与基准按 kind 分开。"""
    if not 0 < coverage_min <= 1 or history_rows < max(HISTORY_REQUIREMENTS.values()):
        raise ValueError("覆盖门槛须在 (0,1] 且行情窗口至少320根")
    sessions = sessions_ending(as_of, history_rows)
    session_set = set(sessions)
    active = universe.active(as_of)
    wanted = {("stock", symbol) for symbol in active} | {("benchmark", symbol) for symbol in BENCHMARKS}
    selected = tuple(b for b in bars if (b.kind, b.symbol) in wanted and sessions[0] <= b.session_date <= as_of)
    issues = [issue for issue in source_issues if (issue.kind, issue.symbol) in wanted]
    duplicates = Counter((b.kind, b.symbol, b.session_date) for b in selected)
    for (kind, symbol, day), count in sorted(duplicates.items()):
        if count > 1:
            issues.append(QualityIssue(symbol=symbol, kind=kind, reason=f"duplicate_session:{day}"))
    invalid = {(issue.kind, issue.symbol) for issue in issues}
    for bar in selected:
        if bar.session_date not in session_set:
            invalid.add((bar.kind, bar.symbol))
            issues.append(QualityIssue(symbol=bar.symbol, kind=bar.kind, reason=f"non_session:{bar.session_date}"))
    clean = tuple(
        sorted(
            (b for b in selected if (b.kind, b.symbol) not in invalid), key=lambda b: (b.kind, b.symbol, b.session_date)
        )
    )
    quality = assess_quality(universe, classifications, clean, as_of, sessions, coverage_min, issues, extra_limitations)
    content = dict(
        as_of=as_of.isoformat(),
        universe=universe.model_dump(mode="json"),
        classifications=classifications.model_dump(mode="json"),
        bars=[b.model_dump(mode="json") for b in clean],
        quality=quality.model_dump(mode="json"),
        source_identity=source_identity,
    )
    return Snapshot(**content, content_hash=digest(content))


def assess_quality(
    universe: UniverseVersion,
    classifications: ClassificationVersion,
    bars: tuple[Bar, ...],
    as_of: date,
    sessions: list[date],
    threshold: float,
    issues: list[QualityIssue],
    extra_limitations: tuple[str, ...] = (),
) -> QualityReport:
    """覆盖仅扣除有证据停牌；历史不足按策略排除，分类缺失不伪造。"""
    active = universe.active(as_of)
    dates = defaultdict(set)
    for bar in bars:
        dates[bar.kind, bar.symbol].add(bar.session_date)
    industries = {
        r.symbol for r in classifications.records if r.effective_date <= as_of and r.industry and r.industry.strip()
    }
    suspended = {
        symbol
        for symbol, members in active.items()
        if any(m.confirmed_suspension and m.suspension_session == as_of for m in members)
    }
    eligible = {strategy: [] for strategy in HISTORY_REQUIREMENTS}
    aligned = 0
    for symbol in active:
        if symbol in suspended:
            issues.append(QualityIssue(symbol=symbol, kind="stock", reason="confirmed_suspension"))
            continue
        if as_of not in dates["stock", symbol]:
            issues.append(QualityIssue(symbol=symbol, kind="stock", reason="missing_latest_session"))
            continue
        aligned += 1
        if symbol not in industries:
            issues.append(QualityIssue(symbol=symbol, kind="stock", reason="missing_industry"))
            continue
        for strategy, count in HISTORY_REQUIREMENTS.items():
            if set(sessions[-count:]) <= dates["stock", symbol]:
                eligible[strategy].append(symbol)
            else:
                issues.append(
                    QualityIssue(symbol=symbol, kind="stock", reason=f"incomplete_history:{count}", strategy=strategy)
                )
    benchmarks_ready = True
    for symbol in BENCHMARKS:
        if not set(sessions) <= dates["benchmark", symbol]:
            benchmarks_ready = False
            issues.append(QualityIssue(symbol=symbol, kind="benchmark", reason="incomplete_benchmark_history"))
    denominator = len(active) - len(suspended)
    coverage = aligned / denominator if denominator else 0.0
    limitations = set(universe.limitations + classifications.limitations + extra_limitations)
    if any(m.obtained_time.date() > as_of for m in universe.members):
        limitations.add("成分取得时间晚于截止日；当前版本回溯可能含存活偏差，非严格历史时点股票池")
    if any(r.obtained_time.date() > as_of for r in classifications.records):
        limitations.add("分类取得时间晚于截止日；历史研究使用后取得资料，非严格历史时点分类")
    return QualityReport(
        passed=bool(denominator and coverage >= threshold and benchmarks_ready),
        expected_session=as_of,
        window_start=sessions[0],
        history_rows=len(sessions),
        universe_count=len(active),
        suspended_count=len(suspended),
        aligned_count=aligned,
        coverage=coverage,
        threshold=threshold,
        eligible={key: tuple(value) for key, value in eligible.items()},
        issues=tuple(sorted(issues, key=lambda i: (i.kind, i.symbol, i.strategy, i.reason))),
        limitations=tuple(sorted(limitations)),
    )
