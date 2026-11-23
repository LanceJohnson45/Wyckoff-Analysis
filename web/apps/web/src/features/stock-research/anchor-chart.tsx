import { lazy, Suspense, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { KlineRow } from '@wyckoff/shared'
import { readResearchAnchorChart, type ResearchReport } from './api'
import { anchorLabels, type ResearchAnchorChart } from './anchor-contract'
import type { EvidenceChartLine } from '@/components/kline-evidence-overlay'

const PriceChart = lazy(async () => ({ default: (await import('@/components/kline-chart')).KlineChart }))
export function researchAnchorLines(chart: ResearchAnchorChart | undefined): EvidenceChartLine[] {
  if (!chart || chart.selected_index === null) return []
  const selected = chart.anchors[chart.selected_index]!
  return [{ label: `${anchorLabels[selected.kind]} · 日线估算`, color: '#8b5cf6', confirmedDate: selected.confirmed_date, points: chart.points.flatMap(point => point.value === null ? [] : [{ date: point.session_date, value: point.value }]) }]
}

export function ResearchAnchorChartView({ report, symbol, data }: { report: ResearchReport; symbol: string; data: KlineRow[] }) {
  const [index, setIndex] = useState<number | null>(null)
  const [visible, setVisible] = useState(true)
  const query = useQuery({ queryKey: ['research', 'anchors', report.report_id, symbol, index], queryFn: ({ signal }) => readResearchAnchorChart(report, symbol, index, signal), retry: false, refetchOnWindowFocus: false })
  const chart = query.data
  const lines = useMemo(() => visible ? researchAnchorLines(chart) : [], [visible, chart])
  const selected = chart?.selected_index === null || chart?.selected_index === undefined ? undefined : chart.anchors[chart.selected_index]
  return <div className="space-y-3">
    {query.isPending && <p role="status" className="text-xs text-muted-foreground">正在读取原产物锚点…</p>}
    {query.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{query.error.message}<button type="button" onClick={() => { void query.refetch() }} className="ml-2 underline">重试锚点查询</button></p>}
    {chart && <AnchorChartControls chart={chart} visible={visible} onVisible={setVisible} onSelect={setIndex} />}
    <Suspense fallback={<p role="status" className="text-xs text-muted-foreground">正在加载行情图…</p>}><PriceChart data={data} priceOnly evidenceLines={lines} height={340} /></Suspense>
    {selected && <p className="break-all text-[11px] text-muted-foreground">原锚点 {selected.date} · 确认 {selected.confirmed_date} · 截止日估算 {selected.daily_estimate === null ? '缺失' : selected.daily_estimate.toLocaleString('zh-CN', { maximumFractionDigits: 6 })} · <code>{selected.ref_prefix}</code></p>}
    <p className="text-[11px] text-muted-foreground">价格与成交量来自固定快照。锚点累计均价为日线估算，曲线从确认日显示，硬过滤关闭；日线不能证明盘中触发或成交。均线与支撑依据下方冻结事实。</p>
  </div>
}

export function AnchorChartControls({ chart, visible, onVisible, onSelect }: { chart: ResearchAnchorChart; visible: boolean; onVisible: (value: boolean) => void; onSelect: (value: number) => void }) {
  if (!chart.calculation_available) return <p className="text-xs text-muted-foreground">本次缺少Martin有效计算，不生成锚点曲线。</p>
  if (chart.anchors.length === 0) return <p className="text-xs text-muted-foreground">原产物没有已确认锚点。</p>
  return <div className="flex flex-wrap items-center gap-3 text-xs"><label className="flex items-center gap-2"><input type="checkbox" checked={visible} onChange={event => onVisible(event.target.checked)} />显示已确认锚点估算</label><label className="flex items-center gap-2">原锚点<select aria-label="选择已确认锚点" value={chart.selected_index!} onChange={event => onSelect(Number(event.target.value))} className="max-w-full rounded-lg border border-border bg-background px-2 py-1">{[...chart.anchors].reverse().map(row => <option key={row.index} value={row.index}>{anchorLabels[row.kind]} {row.date} · 确认 {row.confirmed_date}</option>)}</select></label>{chart.points.every(row => row.value === null) && <p className="text-muted-foreground">所选曲线没有有效累计成交量，不绘制均价。</p>}</div>
}
