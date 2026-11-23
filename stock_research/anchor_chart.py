"""从原锚点及完整冻结行情投影曲线，不重新识别结构或修改策略产物。"""

import math

from stock_research.analysis import MartinReport
from stock_research.errors import ResearchInputError, ResearchIntegrityError, ResearchNotFound
from stock_research.models import Bar
from stock_research.observations import FrozenResearch, frozen_scope
from stock_research.queries import company_views
from stock_research.run_store import RunStore
from stock_research.schemas.anchor_chart import AnchorChart, AnchorDefinition, AnchorPrice


def anchor_directory(analysis: MartinReport, symbol: str, bars: tuple[Bar, ...]) -> tuple[AnchorDefinition, ...]:
    """原索引保留为证据路径；确认时点必须与该产物的右侧窗口一致。"""
    observation = next((row for row in analysis.observations if row.symbol == symbol), None)
    if observation is None:
        return ()
    dates = [row.session_date for row in bars]
    result = []
    try:
        for index, raw in enumerate(observation.anchors):
            anchor = AnchorDefinition(**raw, index=index, ref_prefix=f"martin/{symbol}/anchors/{index}")
            position = dates.index(anchor.date)
            confirmation = position + (0 if anchor.kind == "breakout" else analysis.parameters.swing_right)
            if confirmation >= len(dates) or dates[confirmation] != anchor.confirmed_date:
                raise ValueError("anchor confirmation")
            result.append(anchor)
    except (ValueError, TypeError):
        raise ResearchIntegrityError("冻结锚点的内容或确认日期无效") from None
    return tuple(result)


def anchor_prices(bars: tuple[Bar, ...], anchor: AnchorDefinition, limit: int) -> tuple[AnchorPrice, ...]:
    """累计从真实锚点起算，显示从确认日起算；末值只与原产物核对。"""
    segment = [row for row in bars if row.session_date >= anchor.date]
    volumes = [row.volume for row in segment]
    weighted = [(row.high + row.low + row.close) / 3 * row.volume for row in segment]
    total = sum(volumes)
    estimate = sum(weighted) / total if total > 0 else None
    if (estimate is None) != (anchor.daily_estimate is None) or (
        estimate is not None and not math.isclose(estimate, anchor.daily_estimate, rel_tol=1e-10, abs_tol=1e-10)
    ):
        raise ResearchIntegrityError("锚点估算值与冻结行情不一致")
    cumulative_volume, cumulative_weight = 0.0, 0.0
    points = []
    first_visible = bars[-limit].session_date if len(bars) >= limit else bars[0].session_date
    for row, volume, value in zip(segment, volumes, weighted):
        cumulative_volume += volume
        cumulative_weight += value
        if row.session_date >= max(first_visible, anchor.confirmed_date):
            price = cumulative_weight / cumulative_volume if cumulative_volume > 0 else None
            points.append(AnchorPrice(session_date=row.session_date, value=price))
    return tuple(points)


def anchor_from_scope(scope: FrozenResearch, symbol: str, limit: int, index: int | None) -> AnchorChart:
    """目录与一条曲线共享固定报告上下文，避免把所有曲线灌入问答工具。"""
    snapshot, analysis = scope.snapshot, scope.analyses["martin"][1]
    if symbol not in snapshot.universe.active(snapshot.as_of):
        raise ResearchNotFound("证券不在本次冻结股票池")
    bars = tuple(
        sorted(
            (row for row in snapshot.bars if row.kind == "stock" and row.symbol == symbol),
            key=lambda row: row.session_date,
        )
    )
    anchors = anchor_directory(analysis, symbol, bars)
    if index is not None and (isinstance(index, bool) or index < 0 or index >= len(anchors)):
        raise ResearchInputError("所选锚点不属于本次冻结产物")
    selected = (
        anchors[index]
        if index is not None
        else max(anchors, key=lambda row: (row.confirmed_date, row.date, row.index), default=None)
    )
    return AnchorChart(
        report_id=scope.report_id,
        run_id=scope.view.run_id,
        snapshot_id=snapshot.id,
        snapshot_hash=snapshot.content_hash,
        as_of=snapshot.as_of,
        target_session=scope.view.target_session,
        company=company_views(snapshot, {symbol})[0],
        calculation_available=any(row.symbol == symbol for row in analysis.observations),
        anchors=anchors,
        selected_index=selected.index if selected else None,
        points=anchor_prices(bars, selected, limit) if selected else (),
        limitations=("原锚点的日线累计典型价成交量均价显示重建；辅助证据，硬过滤关闭，不证明盘中触发或成交",),
    )


async def anchor_chart(store: RunStore, report_id: str, symbol: str, limit: int, index: int | None) -> AnchorChart:
    """只读取原报告，绝不联网、更新行情、启动量化或模型。"""
    return anchor_from_scope(await frozen_scope(store, None, report_id), symbol, limit, index)
