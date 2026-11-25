import { useRef, useState } from 'react'
import { useQuery, useQueryClient, type UseQueryResult } from '@tanstack/react-query'
import { ResearchApiError, controlExperiment, createExperiment, readCurrentParameters, readExperiment, readExperimentResult, readExperiments, submissionUnconfirmed } from './api'
import { businessNumber, experimentActive, type CreateExperiment, type ExperimentJob } from './evaluation-contract'

const button = 'rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted disabled:opacity-40'
type Action = { kind: 'create'; body: CreateExperiment } | { kind: 'confirm' } | { kind: 'cancel' | 'retry' | 'read'; id: string }
type Intent = { body: CreateExperiment; key: string }
type ExperimentHistoryQuery = UseQueryResult<Awaited<ReturnType<typeof readExperiments>>>

async function executeAction(action: Action, intent: Intent | null): Promise<ExperimentJob> {
  if (action.kind === 'create' || action.kind === 'confirm') {
    if (!intent) throw new Error('原提交尚未固定，请重新读取参数实验记录')
    return createExperiment(intent.body, intent.key)
  }
  return action.kind === 'read' ? readExperiment(action.id) : controlExperiment(action.id, action.kind)
}

function useActions(accept: (job: ExperimentJob) => void) {
  const guard = useRef(false), original = useRef<Intent | null>(null)
  const [busy, setBusy] = useState<Action['kind'] | null>(null), [pending, setPending] = useState<CreateExperiment | null>(null)
  const [uncertain, setUncertain] = useState(false), [readbackId, setReadbackId] = useState<string | null>(null), [message, setMessage] = useState('')
  async function act(action: Action) {
    if (guard.current || original.current && action.kind !== 'confirm' || readbackId && (action.kind !== 'read' || action.id !== readbackId)) return
    if (action.kind === 'confirm' && !original.current) return
    if (action.kind === 'create') { original.current = { body: { ...action.body }, key: crypto.randomUUID() }; setPending(original.current.body) }
    guard.current = true; setBusy(action.kind); setMessage('')
    const submission = action.kind === 'create' || action.kind === 'confirm'
    try {
      const job = await executeAction(action, original.current)
      accept(job)
      if (submission) { original.current = null; setPending(null); setUncertain(false) }
      setReadbackId(null)
      setMessage(submission ? '参数实验已受理，同一冻结输入会按三个变体分别留档。' : action.kind === 'read' ? '已读取原参数实验状态。' : action.kind === 'cancel' ? '已核对取消请求；实际状态如下。' : '已受理原参数实验续跑，期限与输入保持。')
    } catch (error) {
      const unknown = submissionUnconfirmed(error, action.kind === 'confirm')
      if (submission) { setUncertain(unknown); if (!unknown) { original.current = null; setPending(null) } }
      else if (unknown && 'id' in action) setReadbackId(action.id)
      setMessage(error instanceof Error ? error.message : '参数实验操作尚未确认')
    } finally { guard.current = false; setBusy(null) }
  }
  return { act, busy, pending, uncertain, readbackId, message, blocked: busy !== null || uncertain || readbackId !== null }
}

export function ResearchParameterExperiments({ initialCutoff }: { initialCutoff?: string }) {
  const [opened, setOpened] = useState(false), [active, setActive] = useState(false)
  return <details className="rounded-2xl border border-border bg-card p-5" onToggle={event => { setActive(event.currentTarget.open); if (event.currentTarget.open) setOpened(true) }}>
    <summary className="cursor-pointer text-sm font-semibold">参数实验 · 首轮受控变体</summary>
    {opened && <ExperimentWorkspace active={active} initialCutoff={initialCutoff} />}
  </details>
}

function ExperimentWorkspace({ active, initialCutoff }: { active: boolean; initialCutoff?: string }) {
  const client = useQueryClient()
  const [page, setPage] = useState(1), [selected, setSelected] = useState<string | null>(null)
  const parameters = useQuery({ queryKey: ['research', 'current-parameters'], queryFn: ({ signal }) => readCurrentParameters(signal), enabled: active, retry: false, refetchOnWindowFocus: false })
  const profile = parameters.data?.items.find(item => item.strategy === 'martin')?.parameters.version
  const history = useQuery({ queryKey: ['research', 'parameter-experiments', page], queryFn: ({ signal }) => readExperiments(page, signal), enabled: active, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data?.items.some(experimentActive) ? 5000 : false })
  const job = useQuery({ queryKey: ['research', 'parameter-experiment', selected], queryFn: ({ signal }) => readExperiment(selected!, signal), enabled: active && selected !== null, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data && experimentActive(query.state.data) ? 1000 : false })
  const resultId = job.data?.result_id
  const result = useQuery({ queryKey: ['research', 'parameter-experiment-result', resultId, selected], queryFn: ({ signal }) => readExperimentResult(resultId!, selected!, signal), enabled: Boolean(active && resultId && selected && job.data?.status === 'completed'), retry: false, staleTime: Infinity, refetchOnWindowFocus: false })
  const actions = useActions(value => {
    setSelected(value.id); setPage(1)
    client.setQueryData(['research', 'parameter-experiment', value.id], value)
    void client.invalidateQueries({ queryKey: ['research', 'parameter-experiments'] })
  })
  return <div className="mt-4 space-y-5">
    <p className="text-xs leading-relaxed text-muted-foreground">同一冻结输入顺序运行三个显式 Martin 变体；当前股票池与分类回填历史截面，存在存活偏差。结果只作描述性观察，不发布生产参数，也不构成调参依据。</p>
    <ExperimentForm initialCutoff={initialCutoff} profile={profile} parametersPending={parameters.isPending} state={{ history, actions }} />
    <ExperimentHistory state={{ history, page, selected, setSelected, setPage }} />
    {job.isPending && selected && <p role="status" className="text-xs">正在读取参数实验任务…</p>}
    {job.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{job.error.message}</p>}
    {job.data && <ExperimentJobView job={job.data} actions={actions} />}
    {result.isFetching && <p role="status" className="text-xs">正在读取参数实验结果…</p>}
    {result.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{result.error.message}</p>}
    {result.data && <ExperimentResultView result={result.data} />}
  </div>
}

function ExperimentForm({ initialCutoff, profile, parametersPending, state }: { initialCutoff?: string; profile?: string; parametersPending: boolean; state: { history: ExperimentHistoryQuery; actions: ReturnType<typeof useActions> } }) {
  const [cutoff, setCutoff] = useState(initialCutoff || '')
  const [field, setField] = useState<CreateExperiment['field']>('support_atr')
  const [values, setValues] = useState('0.5,0.75,1.0')
  const parsed = values.split(',').map(value => Number(value))
  const valid = parsed.length === 3 && parsed.every(value => Number.isFinite(value)) && new Set(parsed).size === 3
  const actions = state.actions
  const disabled = actions.blocked || !profile || !cutoff || !valid
  const submit = () => { if (profile && valid) void actions.act({ kind: 'create', body: { profile, cutoff: actions.pending?.cutoff || cutoff, field: actions.pending?.field || field, values: (actions.pending?.values || parsed) as [number, number, number] } }) }
  return <section className="space-y-3" aria-label="新建参数实验">
    {parametersPending && <p role="status" className="text-xs">正在读取当前发布参数…</p>}
    {!parametersPending && !profile && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">当前发布参数不可用，不能创建实验。</p>}
    <p className="break-all text-xs text-muted-foreground">使用当前发布参数版本：{profile || '尚未读取'}。字段固定为 {actions.pending?.field || field}，三个值不得重复。</p>
    <form className="flex flex-wrap items-end gap-3" onSubmit={event => { event.preventDefault(); submit() }}>
      <label className="space-y-1 text-xs"><span className="block text-muted-foreground">评估截止美国交易日</span><input aria-label="评估截止美国交易日" type="date" required value={actions.pending?.cutoff || cutoff} disabled={actions.blocked} onChange={event => setCutoff(event.target.value)} className="h-9 rounded-lg border border-border bg-background px-3 disabled:opacity-50" /></label>
      <label className="space-y-1 text-xs"><span className="block text-muted-foreground">实验字段</span><select aria-label="实验字段" value={actions.pending?.field || field} disabled={actions.blocked} onChange={event => setField(event.target.value as CreateExperiment['field'])} className="h-9 rounded-lg border border-border bg-background px-3 disabled:opacity-50">{['support_atr', 'extension_atr', 'contraction_ratio'].map(item => <option key={item} value={item}>{item}</option>)}</select></label>
      <label className="space-y-1 text-xs"><span className="block text-muted-foreground">三个值（英文逗号分隔）</span><input aria-label="三个实验值" required value={actions.pending?.values.join(',') || values} disabled={actions.blocked} onChange={event => setValues(event.target.value)} className="h-9 w-48 rounded-lg border border-border bg-background px-3 disabled:opacity-50" /></label>
      <button type="submit" disabled={disabled} className={button}>{actions.busy === 'create' ? '正在提交…' : '创建参数实验'}</button>
    </form>
    <div className="flex flex-wrap gap-2">
      <button type="button" className={button} onClick={() => { void state.history.refetch() }}>重新读取参数实验</button>
      {actions.uncertain && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'confirm' }) }}>核对原提交</button>}
      {actions.readbackId && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'read', id: actions.readbackId! }) }}>读取原任务以核对操作</button>}
    </div>
    {actions.uncertain && <p className="text-xs text-amber-700 dark:text-amber-300">提交结果尚未确认，表单已固定。核对将重用原请求，避免重复创建；请保持此页面。</p>}
    {actions.message && <p role="status" className="text-xs leading-relaxed text-amber-700 dark:text-amber-300">{actions.message}</p>}
  </section>
}

function ExperimentHistory({ state }: { state: { history: ExperimentHistoryQuery; page: number; selected: string | null; setSelected: (id: string) => void; setPage: (page: number) => void } }) {
  const history = state.history
  return <section className="space-y-3 border-t border-border pt-4" aria-label="参数实验记录"><h3 className="text-sm font-medium">实验记录</h3>
    {history.isPending && <p role="status" className="text-xs">正在读取参数实验记录…</p>}
    {history.error && !(history.error instanceof ResearchApiError && history.error.status === 404) && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{history.error.message}</p>}
    {history.data && <><div className="grid gap-2 md:grid-cols-2">{history.data.items.map(job => <button type="button" key={job.id} aria-pressed={job.id === state.selected} className={`min-w-0 rounded-xl border p-3 text-left text-xs ${job.id === state.selected ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted'}`} onClick={() => { state.setSelected(job.id); state.setPage(1) }}>
      <span className="block font-medium">{businessNumber(job.create_time, job.id)} · {job.status}</span><span className="mt-1 block break-all text-muted-foreground">{job.field} {job.values.join(' / ')} · 截至 {job.cutoff} · 尝试 {job.attempt_count}/2</span>
    </button>)}</div>
      {history.data.total === 0 && <p className="text-xs text-muted-foreground">尚无参数实验。填写截止交易日、字段和三个显式值后创建。</p>}
      <div className="flex items-center justify-between gap-2 text-xs"><button type="button" disabled={state.page === 1 || history.isFetching} className={button} onClick={() => state.setPage(state.page - 1)}>上一页实验</button><span>{state.page} 页 · 共 {history.data.total} 次</span><button type="button" disabled={state.page * history.data.page_size >= history.data.total || history.isFetching} className={button} onClick={() => state.setPage(state.page + 1)}>下一页实验</button></div>
    </>}
  </section>
}

function ExperimentJobView({ job, actions }: { job: ExperimentJob; actions: ReturnType<typeof useActions> }) {
  const active = experimentActive(job)
  return <section className="space-y-3 rounded-xl border border-border p-4" aria-label="所选参数实验任务">
    <h3 className="text-sm font-medium">{businessNumber(job.create_time, job.id)} · {job.status}</h3>
    <p className="break-all text-xs text-muted-foreground">{job.profile} · {job.field} {job.values.join(' / ')} · 尝试 {job.attempt_count}/2</p>
    {job.cancel_requested && active && <p role="status" className="text-xs">已请求取消，正在等待计算进程实际退出。</p>}
    {job.error_code && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">任务未完成：{job.error_code}。请核对冻结输入后按原输入续跑。</p>}
    <div className="flex flex-wrap gap-2">
      {active && !job.cancel_requested && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'cancel', id: job.id }) }} className={button}>取消所选实验</button>}
      {['failed', 'cancelled'].includes(job.status) && job.attempt_count < 2 && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'retry', id: job.id }) }} className={button}>按原输入续跑</button>}
    </div>
  </section>
}

function ExperimentResultView({ result }: { result: Awaited<ReturnType<typeof readExperimentResult>> }) {
  return <section className="space-y-4 border-t border-border pt-4" aria-label="参数实验结果">
    <h3 className="text-sm font-medium">{result.field} 变体 · 截至 {result.cutoff}</h3>
    <p className="text-xs text-muted-foreground">人工审查 pending；封存组不输出表现，以下只显示候选池规模和可用截面。</p>
    <div className="grid gap-2 md:grid-cols-3">{result.variants.map(variant => <div key={variant.parameter_hash} className="rounded-xl border border-border p-3 text-xs">
      <p className="font-medium">{variant.value}</p><p className="mt-1 text-muted-foreground">候选 {variant.candidate_count}</p>
      <p className="mt-1 text-muted-foreground">research {variant.usable_sections.research ?? 0} · validation {variant.usable_sections.validation ?? 0} · sealed {variant.usable_sections.sealed ?? 0}</p>
      <details className="mt-2"><summary className="cursor-pointer text-muted-foreground">参数与产物摘要</summary><p className="mt-2 break-all">{variant.parameter_hash}</p><p className="break-all">{variant.report_hash}</p></details>
    </div>)}</div>
    {result.limitations.map((text, index) => <p key={index} className="text-xs leading-relaxed text-muted-foreground">{text}</p>)}
  </section>
}
