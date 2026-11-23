import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { readResearchUniverse, type ResearchReport } from './api'
import type { UniversePage, UniverseStock } from './universe-contract'
import { strategyLabels } from './state'

const indices = { sp500: '标普500', nasdaq100: '纳指100', manual: '自选' }

export function ResearchUniverse({ report, selected, onSelect }: { report: ResearchReport; selected: string; onSelect: (symbol: string) => void }) {
  const [draft, setDraft] = useState('')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(1)
  const query = useQuery({ queryKey: ['research', 'universe', report.report_id, search, page], queryFn: ({ signal }) => readResearchUniverse(report, page, search, signal), retry: false, refetchOnWindowFocus: false })
  return <section className="space-y-3 border-b border-border pb-5" aria-label="冻结股票池浏览">
    <div><h4 className="text-sm font-medium">浏览本报告完整股票池</h4><p className="mt-1 text-xs text-muted-foreground">按代码或公司名称搜索。缺行情、缺分类及未计算成员保留；点击公司读取下方固定事实。</p></div>
    <form className="flex flex-wrap gap-2" onSubmit={event => { event.preventDefault(); setSearch(draft.trim()); setPage(1) }}><label className="sr-only" htmlFor="research-universe-search">搜索冻结代码或公司名称</label><input id="research-universe-search" maxLength={100} value={draft} onChange={event => setDraft(event.target.value)} placeholder="代码或公司名称" className="h-9 min-w-0 flex-1 rounded-lg border border-border bg-background px-3 text-xs" /><button type="submit" className="rounded-lg border border-border px-3 text-xs">搜索股票池</button><button type="button" onClick={() => { setDraft(''); setSearch(''); setPage(1) }} className="rounded-lg border border-border px-3 text-xs">显示全部</button></form>
    {query.isPending && <p role="status" className="text-xs text-muted-foreground">正在读取完整冻结股票池…</p>}
    {query.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{query.error.message}<button type="button" onClick={() => { void query.refetch() }} className="ml-3 underline">重试股票池</button></p>}
    {query.data && <><p className="text-xs text-muted-foreground">冻结整池 {query.data.universe_total} 只 · {query.data.search ? `搜索“${query.data.search}”命中` : '当前范围'} {query.data.total} 只 · 第 {query.data.page} 页</p><UniverseTable data={query.data} selected={selected} onSelect={onSelect} /><div className="flex gap-4 text-xs"><button type="button" disabled={page === 1 || query.isFetching} onClick={() => setPage(page - 1)} className="disabled:opacity-40">上一页</button><button type="button" disabled={page * query.data.page_size >= query.data.total || query.isFetching} onClick={() => setPage(page + 1)} className="disabled:opacity-40">下一页</button></div><details className="text-[11px] text-muted-foreground"><summary className="cursor-pointer">股票池版本与限制</summary><p className="mt-2 break-all">股票池 {query.data.universe_version} · 分类 {query.data.classification_version}</p>{query.data.limitations.map((text, index) => <p key={index} className="mt-1">{text}</p>)}</details></>}
  </section>
}

export function UniverseTable({ data, selected, onSelect }: { data: UniversePage; selected: string; onSelect: (symbol: string) => void }) {
  if (data.items.length === 0) return <p className="rounded-lg bg-muted/30 p-4 text-xs text-muted-foreground">当前搜索或页码没有匹配股票，可显示全部或返回前页。</p>
  return <div className="overflow-x-auto rounded-lg border border-border"><table className="w-full min-w-[640px] text-left text-xs"><caption className="sr-only">本报告冻结成员及两策略状态</caption><thead className="bg-muted/40 text-muted-foreground"><tr>{['公司与代码', '行业与主题', '快照日线', 'Martin', 'Wyckoff'].map(title => <th key={title} scope="col" className="p-3 font-medium">{title}</th>)}</tr></thead><tbody>{data.items.map(row => <tr key={row.company.symbol} className="border-t border-border align-top"><th scope="row" className="max-w-52 p-3 font-normal"><button type="button" aria-pressed={selected === row.company.symbol} onClick={() => onSelect(row.company.symbol)} className="text-left hover:underline aria-pressed:font-semibold"><span className="block">{row.company.name}</span><code className="mt-1 block text-[11px] text-muted-foreground">{row.company.symbol}</code></button><p className="mt-2 text-[10px] text-muted-foreground">{row.indices.map(index => indices[index]).join(' · ')}</p></th><td className="max-w-44 p-3"><p>{row.industry || '未取得行业分类'}</p><p className="mt-1 text-[10px] text-muted-foreground">{row.themes.join(' · ') || '暂无已确认主题'}</p></td><td className="p-3"><p>{row.last_session || '无可用行情'}</p><p className="mt-1 text-[11px] text-muted-foreground">{row.history_rows} 根</p></td>{(['martin', 'wyckoff'] as const).map(strategy => <td key={strategy} className="p-3"><UniverseStatus value={row.strategies.find(item => item.strategy === strategy)!} /></td>)}</tr>)}</tbody></table></div>
}

function UniverseStatus({ value }: { value: UniverseStock['strategies'][number] }) {
  return <div className="space-y-1 text-[11px]" aria-label={`${strategyLabels[value.strategy]}状态`}><p className={value.data_eligible ? 'text-muted-foreground' : 'text-amber-700 dark:text-amber-300'}>{value.data_eligible ? '数据可用' : '数据不足'}</p><p>{value.calculation_available ? '已计算' : '未计算'} · {value.in_pool ? '量化池内' : '量化池外'}</p><p className="text-muted-foreground">{value.selected === null ? '初选未提交' : value.selected ? '正式初选' : '未正式入选'}</p></div>
}
