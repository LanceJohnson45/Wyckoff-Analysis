import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { readEvaluationSamples, type Strategy } from './api'
import { horizonSchema, type EvaluationHorizon, type EvaluationResult, type EvaluationSample } from './evaluation-contract'
import { strategyLabels } from './state'

const partitionLabels = { research: '研究', validation: '验证', sealed: '封存' }
const percent = (value: number | null) => value === null ? '—' : `${value.toFixed(2)}%`

export function EvaluationResultView({ result, active }: { result: EvaluationResult; active: boolean }) {
  return <section className="space-y-4 border-t border-border pt-4" aria-label="固定历史评估结果">
    <h3 className="text-sm font-medium">量化池观察结果 · 截至 {result.cutoff}</h3>
    <p className="text-xs text-muted-foreground">20 个截面、两策略。去除跨区观察标签后，可用研究 {result.usable_sections.research ?? 0}、验证 {result.usable_sections.validation ?? 0}、封存 {result.usable_sections.sealed ?? 0} 个截面。人工审查待完成，模型初选表现尚未纳入。</p>
    {result.limitations.length > 0 && <ul className="list-disc space-y-1 pl-5 text-xs leading-relaxed text-amber-700 dark:text-amber-300">{result.limitations.map((text, index) => <li key={index}>{text}</li>)}</ul>}
    <EvaluationSamples resultId={result.id} active={active} />
    <details><summary className="cursor-pointer text-xs text-muted-foreground">固定输入与截面审计</summary><div className="mt-3 space-y-2 text-[11px]">
      <p className="break-all">结果 {result.id} · 报告摘要 {result.report_hash}</p><p className="break-all">输入摘要 {result.input_hash} · 来源摘要 {result.source_hash}</p>
      <p className="break-all">Martin参数摘要 {result.parameter_hashes.martin}</p><p className="break-all">Wyckoff参数摘要 {result.parameter_hashes.wyckoff}</p>
      {result.sections.map(item => <p key={`${item.section.as_of}:${item.strategy}`}>{item.section.as_of} · {strategyLabels[item.strategy]} · {partitionLabels[item.section.partition]}{item.section.purged ? ' · 跨区标签已剔除' : ''} · 股票池 {item.universe_count} / 合格 {item.eligible_count} / 观察 {item.observation_count}</p>)}
    </div></details>
  </section>
}

function EvaluationSamples({ resultId, active }: { resultId: string; active: boolean }) {
  const [page, setPage] = useState(1), [strategy, setStrategy] = useState<Strategy | undefined>('martin')
  const [horizon, setHorizon] = useState<EvaluationHorizon>(20)
  const samples = useQuery({ queryKey: ['research', 'evaluation-samples', resultId, page, strategy], queryFn: ({ signal }) => readEvaluationSamples(resultId, page, strategy, signal), enabled: active, retry: false, staleTime: Infinity, refetchOnWindowFocus: false })
  return <div className="space-y-3">
    <div className="flex flex-wrap gap-3 text-xs">
      <label>观察策略 <select aria-label="历史观察策略" value={strategy || ''} onChange={event => { setStrategy(event.target.value === 'martin' ? 'martin' : event.target.value === 'wyckoff' ? 'wyckoff' : undefined); setPage(1) }} className="ml-2 rounded-lg border border-border bg-background p-2"><option value="martin">Martin 趋势回调</option><option value="wyckoff">Wyckoff 量价结构</option><option value="">全部策略</option></select></label>
      <label>后续窗口 <select aria-label="历史观察窗口" value={horizon} onChange={event => setHorizon(horizonSchema.parse(Number(event.target.value)))} className="ml-2 rounded-lg border border-border bg-background p-2">{[1, 3, 5, 10, 20].map(days => <option key={days} value={days}>{days} 个交易日</option>)}</select></label>
    </div>
    <p className="text-[11px] leading-relaxed text-muted-foreground">按截面、策略和股票记录观察条目，同一股票可出现多次。净收益已扣工程估算的往返成本；净超额以 SPY 为基准。跨区标签不计入分区结论，封存表现保持未开放。</p>
    {samples.isPending && <p role="status" className="text-xs">正在读取逐股观察…</p>}
    {samples.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{samples.error.message}<button type="button" onClick={() => { void samples.refetch() }} className="ml-3 underline">重试逐股观察</button></p>}
    {samples.data && <><div className="overflow-x-auto"><table className="w-full whitespace-nowrap text-left text-xs"><caption className="sr-only">历史量化池逐股观察</caption><thead><tr className="border-b border-border text-muted-foreground">{['截面 / 股票', '策略 / 分区', '窗口状态', '净收益', '净超额', '最大有利幅度', '最大不利幅度'].map(label => <th key={label} scope="col" className="px-3 py-2 font-medium">{label}</th>)}</tr></thead><tbody>{samples.data.items.map(sample => <EvaluationSampleRow key={sample.id} sample={sample} horizon={horizon} />)}</tbody></table></div>
      {samples.data.total === 0 && <p className="text-xs text-muted-foreground">当前策略没有观察条目；这不代表零收益。</p>}
      <div className="flex items-center justify-between gap-2 text-xs"><button type="button" disabled={page === 1 || samples.isFetching} onClick={() => setPage(page - 1)} className="rounded-lg border border-border px-3 py-2 disabled:opacity-40">上一页观察</button><span>{page} 页 · 共 {samples.data.total} 条</span><button type="button" disabled={page * samples.data.page_size >= samples.data.total || samples.isFetching} onClick={() => setPage(page + 1)} className="rounded-lg border border-border px-3 py-2 disabled:opacity-40">下一页观察</button></div>
    </>}
  </div>
}

export function EvaluationSampleRow({ sample, horizon }: { sample: EvaluationSample; horizon: EvaluationHorizon }) {
  const window = sample.windows?.find(item => item.horizon === horizon)
  const status = sample.partition === 'sealed' ? '封存 · 表现未开放' : window?.status === 'completed' ? '观察完成' : window?.status === 'pending' ? '观察期未满' : '行情缺失'
  return <tr className="border-b border-border/60">
    <th scope="row" className="px-3 py-3 font-normal"><span className="block font-medium">{sample.symbol}</span><span className="text-muted-foreground">{sample.as_of}</span></th>
    <td className="px-3 py-3">{strategyLabels[sample.strategy]}<span className="block text-muted-foreground">{partitionLabels[sample.partition]}{sample.purged && ' · 跨区标签已剔除'}</span></td>
    <td className="px-3 py-3">{status}{window && <span className="block text-[11px] text-muted-foreground">窗口截至 {window.target_session}</span>}{window?.status === 'completed' && <details><summary className="cursor-pointer text-[11px] text-muted-foreground">收益与成本</summary><p className="text-[11px]">毛收益 {percent(window.gross_return_pct)} · 成本 {percent(window.cost_pct)} · SPY {percent(window.benchmark_return_pct)}</p></details>}{window?.status === 'missing_data' && <details><summary className="cursor-pointer text-[11px] text-muted-foreground">查看缺失日期</summary><p className="max-w-64 whitespace-normal text-[11px]">{window.missing_sessions.join('、')}</p></details>}</td>
    {[window?.net_return_pct, window?.net_excess_pct, window?.mfe_pct, window?.mae_pct].map((value, index) => <td key={index} className="px-3 py-3 tabular-nums">{percent(value ?? null)}</td>)}
  </tr>
}
