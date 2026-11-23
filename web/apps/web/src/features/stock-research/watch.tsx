import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { readResearchWatchlist, readResearchWatchEvents, type Strategy } from './api'
import { strategyLabels } from './state'
import type { ResearchWatchItem } from './watch-contract'

const stateLabels = { new: '新增', waiting: '等待', near: '接近观察区', invalid: '已失效' }
const reasonLabels: Record<string, string> = { new_selection: '本日新入选', structure_valid: '结构事实仍有效', structure_invalid: '结构硬条件已失效', production_layer2_not_passed: '生产L2结构条件未通过', production_layer1_not_passed: '尚未到生产结构判断层', calculation_missing: '缺少本次有效计算', selection_not_committed: '本日初选尚未成功提交' }

export function ResearchWatchlists() {
  const [strategy, setStrategy] = useState<Strategy | undefined>()
  const [page, setPage] = useState(1)
  const [eventsOpen, setEventsOpen] = useState(false)
  const [eventPage, setEventPage] = useState(1)
  const items = useQuery({ queryKey: ['research', 'watchlists', strategy, page], queryFn: ({ signal }) => readResearchWatchlist(page, strategy, signal), retry: false, refetchOnWindowFocus: false })
  const events = useQuery({ queryKey: ['research', 'watch-events', strategy, eventPage], queryFn: ({ signal }) => readResearchWatchEvents(eventPage, strategy, signal), enabled: eventsOpen, retry: false, refetchOnWindowFocus: false })
  return <section className="rounded-2xl border border-border bg-card p-5" aria-label="持续研究观察名单">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h3 className="text-sm font-semibold">持续研究名单</h3><p className="mt-2 text-xs text-muted-foreground">以下为当前保存的观察状态，独立于上方固定历史报告；每项显示自己的检查日期。</p></div><label className="text-xs"><span className="sr-only">筛选观察名单策略</span><select aria-label="筛选观察名单策略" value={strategy || 'all'} onChange={(event) => { const value = event.target.value; setStrategy(value === 'all' ? undefined : value === 'martin' ? 'martin' : 'wyckoff'); setPage(1); setEventPage(1) }} className="h-9 rounded-lg border border-border bg-background px-3"><option value="all">所有策略</option><option value="martin">Martin</option><option value="wyckoff">Wyckoff</option></select></label></div>
    {items.isPending && <p role="status" className="mt-4 text-xs text-muted-foreground">正在读取持续名单…</p>}
    {items.error && <p role="alert" className="mt-4 text-xs text-amber-700 dark:text-amber-300">{items.error.message}<button type="button" onClick={() => { void items.refetch() }} className="ml-3 underline">重试名单查询</button></p>}
    {items.data && <><div className="mt-4 grid gap-3 md:grid-cols-2">{items.data.items.map((item) => <WatchItemView key={`${item.strategy}:${item.symbol}`} item={item} />)}</div>{items.data.total === 0 && <p className="mt-4 text-xs text-muted-foreground">尚无已采用的观察名单；空初选、未复核和无历史观察分别保留，不补位。</p>}<PageControls page={page} total={items.data.total} setPage={setPage} /></>}
    <details className="mt-5 border-t border-border pt-4" onToggle={(event) => setEventsOpen(event.currentTarget.open)}><summary className="cursor-pointer text-xs font-medium">名单变化记录</summary>
      {eventsOpen && <div className="mt-3 space-y-2">{events.isPending && <p role="status" className="text-xs text-muted-foreground">正在读取变化事件…</p>}{events.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{events.error.message}<button type="button" onClick={() => { void events.refetch() }} className="ml-3 underline">重试事件查询</button></p>}{events.data && <>{events.data.items.map((event) => <p key={event.id} className="rounded-lg bg-muted/40 p-3 text-xs">{event.target_session} · {strategyLabels[event.strategy]} · {event.symbol} · {event.before ? stateLabels[event.before] : '未观察'} → {stateLabels[event.after.state]}{event.after.data_insufficient ? ' · 数据不足' : event.after.not_reviewed ? ' · 未复核' : ' · 已复核'}</p>)}{events.data.total === 0 && <p className="text-xs text-muted-foreground">尚无变化事件。</p>}<PageControls page={eventPage} total={events.data.total} setPage={setEventPage} /></>}</div>}
    </details><p className="mt-4 text-[11px] leading-relaxed text-muted-foreground">同一交易日第一份成功初选固定为来源；重分析报告供对照，不覆盖已采用名单。缺数据或模型结论保留最后有效状态。研究观察不产生交易许可。</p>
  </section>
}

export function WatchItemView({ item }: { item: ResearchWatchItem }) {
  return <article className="min-w-0 rounded-xl border border-border p-4" aria-label={`${item.symbol}持续研究状态`}>
    <div className="flex flex-wrap items-center justify-between gap-2"><h4 className="text-sm font-medium">{item.name} <code className="ml-2 text-xs text-muted-foreground">{item.symbol}</code></h4><span className="rounded-full bg-muted px-2 py-1 text-xs">{stateLabels[item.state]}</span></div>
    <p className="mt-2 text-xs text-muted-foreground">{strategyLabels[item.strategy]} · 最近检查 {item.target_session}</p>
    <div className="mt-2 flex flex-wrap gap-2 text-xs">{item.data_insufficient && <span className="text-amber-700 dark:text-amber-300">数据不足 · 保留原状态</span>}{item.not_reviewed && <span className="text-amber-700 dark:text-amber-300">本日未复核</span>}</div>
    <p className="mt-2 text-xs">{reasonLabels[item.reason] || '其他研究状态，详见来源记录'}</p><p className="mt-2 text-[11px] text-muted-foreground">有效状态日期 {item.state_session} · 行情 {item.state_as_of}</p>
    <details className="mt-3 break-all text-[11px] text-muted-foreground"><summary className="cursor-pointer">来源与固定引用</summary><p className="mt-2">状态来源 {item.state_run_id}</p><p className="mt-1">最近检查 {item.checked_run_id}</p>{item.evidence_refs.map((ref) => <p key={ref} className="mt-1"><code>{ref}</code></p>)}</details>
  </article>
}

function PageControls({ page, total, setPage }: { page: number; total: number; setPage: (page: number) => void }) {
  return <div className="mt-3 flex items-center justify-between gap-2 text-xs text-muted-foreground"><span>第 {page} 页 · 共 {total} 条</span><div className="flex gap-3"><button type="button" disabled={page <= 1} onClick={() => setPage(page - 1)} className="disabled:opacity-40">上一页</button><button type="button" disabled={page * 10 >= total} onClick={() => setPage(page + 1)} className="disabled:opacity-40">下一页</button></div></div>
}
