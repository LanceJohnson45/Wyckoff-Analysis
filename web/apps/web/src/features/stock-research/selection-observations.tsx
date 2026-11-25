import { useRef, useState } from 'react'
import { useQuery, useQueryClient, type UseQueryResult } from '@tanstack/react-query'
import {
  ResearchApiError,
  controlSelectionObservation,
  createSelectionObservation,
  readSelectionObservation,
  readSelectionObservationResult,
  readSelectionObservationSamples,
  readSelectionObservations,
  submissionUnconfirmed,
} from './api'
import {
  selectionObservationActive,
  interruptedCancellation,
  businessNumber,
  type CreateSelectionObservation,
  type SelectionObservationJob,
} from './evaluation-contract'

const button = 'rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted disabled:opacity-40'

type Action = { kind: 'create'; body: CreateSelectionObservation } | { kind: 'confirm' } | { kind: 'cancel' | 'retry' | 'read'; id: string }
type Intent = { body: CreateSelectionObservation; key: string }
type ObservationHistoryQuery = UseQueryResult<Awaited<ReturnType<typeof readSelectionObservations>>>
type ObservationActions = ReturnType<typeof useActions>

async function executeAction(action: Action, intent: Intent | null): Promise<SelectionObservationJob> {
  if (action.kind === 'create' || action.kind === 'confirm') {
    if (!intent) throw new Error('原提交尚未固定，请重新读取正式观测记录')
    return createSelectionObservation(intent.body, intent.key)
  }
  return action.kind === 'read' ? readSelectionObservation(action.id) : controlSelectionObservation(action.id, action.kind)
}

function useActions(accept: (job: SelectionObservationJob) => void) {
  const guard = useRef(false), original = useRef<Intent | null>(null)
  const [busy, setBusy] = useState<Action['kind'] | null>(null)
  const [pending, setPending] = useState<CreateSelectionObservation | null>(null)
  const [uncertain, setUncertain] = useState(false)
  const [readbackId, setReadbackId] = useState<string | null>(null)
  const [message, setMessage] = useState('')
  async function act(action: Action) {
    if (guard.current || original.current && action.kind !== 'confirm' || readbackId && (action.kind !== 'read' || action.id !== readbackId)) return
    if (action.kind === 'confirm' && !original.current) return
    if (action.kind === 'create') {
      original.current = { body: { ...action.body }, key: crypto.randomUUID() }
      setPending(original.current.body)
    }
    guard.current = true; setBusy(action.kind); setMessage('')
    const submission = action.kind === 'create' || action.kind === 'confirm'
    try {
      const job = await executeAction(action, original.current)
      accept(job)
      if (submission) { original.current = null; setPending(null); setUncertain(false) }
      setReadbackId(null)
      setMessage(submission ? '正式观测已受理，固定报告与输入会分别留档。' : action.kind === 'read' ? '已读取原正式观测状态。' : action.kind === 'cancel' ? '已核对取消请求；实际状态如下。' : '已受理原正式观测续跑，期限与输入保持。')
    } catch (error) {
      const unknown = submissionUnconfirmed(error, action.kind === 'confirm')
      if (submission) {
        setUncertain(unknown)
        if (!unknown) { original.current = null; setPending(null) }
      } else if (unknown && 'id' in action) setReadbackId(action.id)
      setMessage(error instanceof Error ? error.message : '正式观测操作尚未确认')
    } finally { guard.current = false; setBusy(null) }
  }
  return { act, busy, pending, uncertain, readbackId, message, blocked: busy !== null || uncertain || readbackId !== null }
}

export function ResearchSelectionObservations({ reportId, initialCutoff }: { reportId: string; initialCutoff?: string }) {
  const [opened, setOpened] = useState(false), [active, setActive] = useState(false)
  return <details className="rounded-2xl border border-border bg-card p-5" onToggle={event => { setActive(event.currentTarget.open); if (event.currentTarget.open) setOpened(true) }}>
    <summary className="cursor-pointer text-sm font-semibold">正式初选观测 · 提交后开盘表现</summary>
    {opened && <ObservationWorkspace reportId={reportId} active={active} initialCutoff={initialCutoff} />}
  </details>
}

function ObservationWorkspace({ reportId, active, initialCutoff }: { reportId: string; active: boolean; initialCutoff?: string }) {
  const client = useQueryClient()
  const [page, setPage] = useState(1), [selected, setSelected] = useState<string | null>(null)
  const history = useQuery({ queryKey: ['research', 'selection-observations', page], queryFn: ({ signal }) => readSelectionObservations(page, signal), enabled: active, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data?.items.some(selectionObservationActive) ? 5000 : false })
  const job = useQuery({ queryKey: ['research', 'selection-observation', selected], queryFn: ({ signal }) => readSelectionObservation(selected!, signal), enabled: active && selected !== null, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data && selectionObservationActive(query.state.data) ? 1000 : false })
  const resultId = job.data?.result_id
  const result = useQuery({ queryKey: ['research', 'selection-observation-result', resultId, selected], queryFn: ({ signal }) => readSelectionObservationResult(resultId!, selected!, signal), enabled: Boolean(active && resultId && selected && job.data?.status === 'completed'), retry: false, staleTime: Infinity, refetchOnWindowFocus: false })
  const actions = useActions(value => {
    setSelected(value.id); setPage(1)
    client.setQueryData(['research', 'selection-observation', value.id], value)
    void client.invalidateQueries({ queryKey: ['research', 'selection-observations'] })
  })
  return <div className="mt-4 space-y-5">
    <p className="text-xs leading-relaxed text-muted-foreground">观测固定报告当时的实际初选，参考点为提交后首个正式开盘；pending、缺数和空名单都保留，不表示人工审查或策略有效性。</p>
    <ObservationForm reportId={reportId} initialCutoff={initialCutoff} state={{ history, actions }} />
    <ObservationHistory state={{ history, page, selected, setSelected, setPage }} />
    {job.isPending && selected && <p role="status" className="text-xs">正在读取正式观测任务…</p>}
    {job.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{job.error.message}</p>}
    {job.data && <ObservationJobView job={job.data} actions={actions} />}
    {result.isFetching && <p role="status" className="text-xs">正在读取正式观测结果…</p>}
    {result.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{result.error.message}</p>}
    {result.data && <ObservationResultView result={result.data} active={active} />}
  </div>
}

function ObservationForm({ reportId, initialCutoff, state }: { reportId: string; initialCutoff?: string; state: { history: ObservationHistoryQuery; actions: ObservationActions } }) {
  const [cutoff, setCutoff] = useState(initialCutoff || '')
  const actions = state.actions
  const unsupported = state.history.error instanceof ResearchApiError && state.history.error.status === 404
  return <section className="space-y-3" aria-label="新建正式初选观测">
    {unsupported && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">当前研究连接尚未提供正式初选观测，请更新研究服务后重新读取。</p>}
    <p className="break-all text-xs text-muted-foreground">固定报告 {actions.pending?.report_id || reportId}。源报告修订或后续新增初选须创建新观测，旧结果保持可读。</p>
    <form className="flex flex-wrap items-end gap-3" onSubmit={event => { event.preventDefault(); void actions.act({ kind: 'create', body: { report_id: actions.pending?.report_id || reportId, cutoff: actions.pending?.cutoff || cutoff } }) }}>
      <label className="space-y-1 text-xs"><span className="block text-muted-foreground">观测截止美国交易日</span><input aria-label="观测截止美国交易日" type="date" required value={actions.pending?.cutoff || cutoff} disabled={actions.blocked} onChange={event => setCutoff(event.target.value)} className="h-9 rounded-lg border border-border bg-background px-3 disabled:opacity-50" /></label>
      <button type="submit" disabled={actions.blocked || !cutoff} className={button}>{actions.busy === 'create' ? '正在提交…' : '创建正式观测'}</button>
    </form>
    <div className="flex flex-wrap gap-2">
      <button type="button" className={button} onClick={() => { void state.history.refetch() }}>重新读取正式观测</button>
      {actions.uncertain && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'confirm' }) }}>核对原提交</button>}
      {actions.readbackId && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'read', id: actions.readbackId! }) }}>读取原任务以核对操作</button>}
    </div>
    {actions.uncertain && <p className="text-xs text-amber-700 dark:text-amber-300">提交结果尚未确认，表单已固定。核对将重用原请求，避免重复创建；请保持此页面。</p>}
    {actions.message && <p role="status" className="text-xs leading-relaxed text-amber-700 dark:text-amber-300">{actions.message}</p>}
  </section>
}

function ObservationHistory({ state }: { state: { history: ObservationHistoryQuery; page: number; selected: string | null; setSelected: (id: string) => void; setPage: (page: number) => void } }) {
  const history = state.history
  return <section className="space-y-3 border-t border-border pt-4" aria-label="正式初选观测记录"><h3 className="text-sm font-medium">观测记录</h3>
    {history.isPending && <p role="status" className="text-xs">正在读取正式观测记录…</p>}
    {history.error && !(history.error instanceof ResearchApiError && history.error.status === 404) && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{history.error.message}</p>}
    {history.data && <><div className="grid gap-2 md:grid-cols-2">{history.data.items.map(job => <button type="button" key={job.id} aria-pressed={job.id === state.selected} className={`min-w-0 rounded-xl border p-3 text-left text-xs ${job.id === state.selected ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted'}`} onClick={() => { state.setSelected(job.id); state.setPage(1) }}>
      <span className="block font-medium">{businessNumber(job.create_time, job.id)} · {job.status}</span><span className="mt-1 block break-all text-muted-foreground">报告 {job.report_id.slice(0, 8)} · 截至 {job.cutoff} · 尝试 {job.attempt_count}/2</span>
    </button>)}</div>
      {history.data.total === 0 && <p className="text-xs text-muted-foreground">尚无正式观测。当前报告有正式初选后可显式创建。</p>}
      <div className="flex items-center justify-between gap-2 text-xs"><button type="button" disabled={state.page === 1 || history.isFetching} className={button} onClick={() => state.setPage(state.page - 1)}>上一页观测</button><span>{state.page} 页 · 共 {history.data.total} 次</span><button type="button" disabled={state.page * history.data.page_size >= history.data.total || history.isFetching} className={button} onClick={() => state.setPage(state.page + 1)}>下一页观测</button></div>
    </>}
  </section>
}

export function ObservationJobView({ job, actions }: { job: SelectionObservationJob; actions: ObservationActions }) {
  const active = selectionObservationActive(job)
  const cancelled = interruptedCancellation(job)
  return <section className="space-y-3 rounded-xl border border-border p-4" aria-label="所选正式观测任务">
    <h3 className="text-sm font-medium">正式观测 · {job.status}</h3>
    <p className="break-all text-xs text-muted-foreground">报告 {job.report_id} · 尝试 {job.attempt_count}/2</p>
    {job.cancel_requested && active && <p role="status" className="text-xs">已请求取消，正在等待计算进程实际退出。</p>}
    {job.error_code && !cancelled && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">任务未完成：{job.error_code}。请核对固定报告与输入后创建新观测。</p>}
    {cancelled && <p role="status" className="text-xs">已取消；原任务可按原报告和截止日续跑。</p>}
    <div className="flex flex-wrap gap-2">
      {active && !job.cancel_requested && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'cancel', id: job.id }) }} className={button}>取消所选观测</button>}
      {['failed', 'cancelled'].includes(job.status) && job.attempt_count < 2 && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'retry', id: job.id }) }} className={button}>按原输入续跑</button>}
    </div>
  </section>
}

function ObservationResultView({ result, active }: { result: Awaited<ReturnType<typeof readSelectionObservationResult>>; active: boolean }) {
  const [page, setPage] = useState(1)
  const samples = useQuery({ queryKey: ['research', 'selection-observation-samples', result.id, page], queryFn: ({ signal }) => readSelectionObservationSamples(result.id, page, signal), enabled: active, retry: false, staleTime: Infinity, refetchOnWindowFocus: false })
  return <section className="space-y-4 border-t border-border pt-4" aria-label="固定正式初选观测结果">
    <h3 className="text-sm font-medium">正式初选观察结果 · 截至 {result.cutoff}</h3>
    <p className="text-xs text-muted-foreground">入选 {result.selection_count} 份 · 逐股观察 {result.observation_count} 条 · 缺失策略 {result.missing_strategies.length === 0 ? '无' : result.missing_strategies.join('、')} · 人工审查 pending</p>
    {result.limitations.map((text, index) => <p key={index} className="text-xs leading-relaxed text-muted-foreground">{text}</p>)}
    {samples.isPending && <p role="status" className="text-xs">正在读取逐股正式观察…</p>}
    {samples.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{samples.error.message}</p>}
    {samples.data && <><table className="w-full whitespace-nowrap text-left text-xs"><caption className="sr-only">正式初选逐股观察</caption><thead><tr className="border-b border-border text-muted-foreground">{['股票', '策略', '参考开盘', '1日净收益', '3日净收益', '5日净收益', '10日净收益', '20日净收益'].map(label => <th key={label} scope="col" className="px-3 py-2 font-medium">{label}</th>)}</tr></thead><tbody>{samples.data.items.map(sample => <tr key={sample.id} className="border-b border-border/60"><th scope="row" className="px-3 py-3 font-normal">{sample.symbol}</th><td className="px-3 py-3">{sample.strategy}</td><td className="px-3 py-3">{sample.reference_session}</td>{sample.windows.map(window => <td key={window.horizon} className="px-3 py-3 tabular-nums">{window.net_return_pct === null ? window.status === 'pending' ? '未满' : '缺数' : `${window.net_return_pct.toFixed(2)}%`}</td>)}</tr>)}</tbody></table>
      <div className="flex items-center justify-between gap-2 text-xs"><button type="button" disabled={page === 1 || samples.isFetching} className={button} onClick={() => setPage(page - 1)}>上一页正式观察</button><span>{page} 页 · 共 {samples.data.total} 条</span><button type="button" disabled={page * samples.data.page_size >= samples.data.total || samples.isFetching} className={button} onClick={() => setPage(page + 1)}>下一页正式观察</button></div>
    </>}
  </section>
}
