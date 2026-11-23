import { lazy, Suspense, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import type { KlineRow } from '@wyckoff/shared'
import { readResearchStock, type ResearchReport } from './api'
import type { StockEvidence, StockObservation } from './stock-contract'
import { strategyLabels } from './state'
import { ResearchUniverse } from './universe'

const PriceChart = lazy(async () => ({ default: (await import('@/components/kline-chart')).KlineChart }))
const factorLabels: Record<string, string> = { close: '收盘', ema_fast: '短周期EMA', ema_middle: '中周期EMA', ema_slow: '长周期EMA', atr: 'ATR', adr: 'ADR', rs: '相对SPY收益差', extension: '距中周期EMA / ATR', volume_ratio: '短期 / 基准均量', range_ratio: '短期 / 基准均幅' }

export function researchKlines(stock: StockObservation): KlineRow[] {
  return stock.history.map(({ session_date, open, high, low, close, volume }) => ({ date: session_date, open, high, low, close, volume }))
}
export function evidenceValue(value: StockEvidence['value']): string {
  return value === null ? '缺失' : typeof value === 'boolean' ? value ? '是' : '否' : typeof value === 'number' ? value.toLocaleString('zh-CN', { maximumFractionDigits: 6 }) : value
}

export function ResearchStockPanel({ report, onDiscuss }: { report: ResearchReport; onDiscuss: (text: string) => void }) {
  const [input, setInput] = useState(report.run.companies[0]?.symbol || '')
  const [symbol, setSymbol] = useState(input)
  const query = useQuery({ queryKey: ['research', 'stock', report.report_id, symbol], queryFn: ({ signal }) => readResearchStock(report, symbol, signal), enabled: symbol !== '', retry: false, refetchOnWindowFocus: false })
  const choose = (value: string) => { setInput(value); setSymbol(value) }
  const discuss = () => {
    if (!query.data) return
    const stock = query.data
    onDiscuss(`请基于本报告分析${stock.company.symbol}的两策略事实、支持或排除依据、不同意见与待确认条件，并给出实际工具引用。`)
  }
  return <section className="space-y-4 rounded-2xl border border-border bg-card p-5" aria-label="固定报告个股证据">
    <div><h3 className="text-sm font-semibold">个股事实与快照行情</h3><p className="mt-2 text-xs text-muted-foreground">固定于上方报告修订。可查询本次完整股票池；未进入量化池与未正式入选分别表达。</p></div>
    <ResearchUniverse report={report} selected={symbol} onSelect={choose} />
    <form className="flex flex-wrap items-center gap-2" onSubmit={(event) => { event.preventDefault(); setSymbol(input.trim().toUpperCase()) }}><label className="sr-only" htmlFor="research-stock-symbol">美股代码</label><input id="research-stock-symbol" value={input} required maxLength={32} onChange={(event) => setInput(event.target.value)} placeholder="如 ACGL" className="h-9 w-40 rounded-lg border border-border bg-background px-3 text-xs" /><button type="submit" className="h-9 rounded-lg border border-border px-3 text-xs">查询固定事实</button></form>
    {report.run.companies.length > 0 && <div className="flex flex-wrap gap-2">{report.run.companies.map(company => <button key={company.symbol} type="button" aria-pressed={symbol === company.symbol} onClick={() => choose(company.symbol)} className="rounded-full border border-border px-3 py-1 text-xs aria-pressed:bg-muted">{company.name} · {company.symbol}</button>)}</div>}
    {!symbol && <p className="text-xs text-muted-foreground">输入本次股票池内的代码以读取快照。</p>}
    {symbol && query.isPending && <p role="status" className="text-xs text-muted-foreground">正在读取固定股票事实…</p>}
    {query.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{query.error.message}<button type="button" onClick={() => { void query.refetch() }} className="ml-3 underline">重试个股查询</button></p>}
    {query.data && <><StockFactsView stock={query.data} /><button type="button" onClick={discuss} className="rounded-lg border border-border px-3 py-2 text-xs">带此股票证据继续讨论</button></>}
  </section>
}

export function StockFactsView({ stock }: { stock: StockObservation }) {
  return <div className="space-y-4"><header><h4 className="text-base font-semibold">{stock.company.name} <code className="ml-2 text-xs text-muted-foreground">{stock.company.symbol}</code></h4><p className="mt-2 text-xs text-muted-foreground">截止 {stock.as_of} · 显示 {stock.history.length} / {stock.history_total} 根同口径日线</p></header>
    {stock.history.length > 0 ? <Suspense fallback={<p role="status" className="text-xs text-muted-foreground">正在加载行情图…</p>}><PriceChart data={researchKlines(stock)} priceOnly height={340} /></Suspense> : <p className="text-xs text-amber-700 dark:text-amber-300">该股票没有本次可用日线，不绘制行情图。</p>}
    <p className="text-[11px] text-muted-foreground">图表只展示快照价格与成交量。均线、支撑和锚点的策略依据以下方冻结事实为准；日线不能证明盘中触发或成交。</p>
    <div className="grid gap-4 xl:grid-cols-2">{stock.observations.map(observation => <section key={observation.strategy} className="min-w-0 rounded-xl border border-border p-4"><h5 className="text-sm font-medium">{strategyLabels[observation.strategy]} · {observation.in_pool ? '量化池内' : '未进入量化池'}</h5>{!observation.calculation_available && <p className="mt-2 text-xs text-amber-700 dark:text-amber-300">本次缺少有效量化计算，不能推断为零因子或模型否定。</p>}<div className="mt-3 grid grid-cols-2 gap-2">{observation.facts.filter(fact => fact.ref.includes('/factors/') && Object.hasOwn(factorLabels, fact.ref.split('/').at(-1) || '')).map(fact => <div key={fact.ref} className="rounded-lg bg-muted/40 p-2 text-xs"><p className="text-muted-foreground">{factorLabels[fact.ref.split('/').at(-1)!]}</p><p className="mt-1 font-medium tabular-nums" title={String(fact.value)}>{evidenceValue(fact.value)}</p></div>)}</div><FactList title="全部固定因子、条件与锚点" facts={observation.facts} /><FactList title="量化排除证据" facts={observation.exclusions} /><details className="mt-3 break-all text-[11px] text-muted-foreground"><summary className="cursor-pointer">参数与产物版本、数据限制</summary><p className="mt-2">参数摘要 {observation.parameter_hash}</p><p className="mt-1">产物摘要 {observation.content_hash}</p>{observation.limitations.map(text => <p key={text} className="mt-2">{text}</p>)}</details></section>)}</div>
    {stock.groups.length > 0 && <details className="text-xs"><summary className="cursor-pointer">行业与已确认主题 · {stock.groups.length} 组</summary><div className="mt-3 space-y-2">{stock.groups.map(group => <div key={group.name} className="rounded-lg bg-muted/40 p-3"><p>{group.name} · 有效 {group.valid_members.length} / {group.members.length} · 覆盖 {(group.coverage * 100).toFixed(1)}%</p><p className="mt-1 text-muted-foreground">{group.usable ? `组得分 ${evidenceValue(group.score)}` : '成员或覆盖不足，不形成分组得分'}</p><code className="mt-1 block break-all text-[10px]">{group.ref_prefix}</code></div>)}</div></details>}
  </div>
}

function FactList({ title, facts }: { title: string; facts: StockEvidence[] }) {
  return <details className="mt-3 text-xs"><summary className="cursor-pointer text-muted-foreground">{title} · {facts.length} 项</summary><div className="mt-2 max-h-64 space-y-2 overflow-auto">{facts.length === 0 && <p className="text-muted-foreground">未提供此类事实。</p>}{facts.map(fact => <p key={fact.ref} className="break-all"><code className="text-[10px] text-muted-foreground">{fact.ref}</code><span className="ml-2" title={String(fact.value)}>{evidenceValue(fact.value)}</span></p>)}</div></details>
}
