import { useState } from 'react'
import { ResearchApiError } from './api'
import { useEvaluationWorkspace } from './evaluation-state'
import { EvaluationJobView, evaluationStatus } from './evaluation-job'
import { EvaluationResultView } from './evaluation-result'
import { businessNumber } from './evaluation-contract'

type Workspace = ReturnType<typeof useEvaluationWorkspace>
const button = 'rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted disabled:opacity-40'

export function ResearchEvaluations({ initialCutoff }: { initialCutoff?: string }) {
  const [opened, setOpened] = useState(false), [active, setActive] = useState(false)
  return <details className="rounded-2xl border border-border bg-card p-5" onToggle={event => { setActive(event.currentTarget.open); if (event.currentTarget.open) setOpened(true) }}>
    <summary className="cursor-pointer text-sm font-semibold">历史评估 · 量化池回放</summary>
    {opened && <EvaluationWorkspace active={active} initialCutoff={initialCutoff} />}
  </details>
}

function EvaluationWorkspace({ active, initialCutoff }: { active: boolean; initialCutoff?: string }) {
  const state = useEvaluationWorkspace(active)
  return <div className="mt-4 space-y-5">
    <p className="text-xs leading-relaxed text-muted-foreground">回放两策略的历史量化池，保留输入与参数版本。这里只记录观察表现；模型初选、人工审查与参数实验仍需分别完成。</p>
    <EvaluationForm state={state} initialCutoff={initialCutoff} />
    <EvaluationHistory state={state} />
    {state.job.isPending && state.selected && <p role="status" className="text-xs">正在读取评估任务…</p>}
    {state.job.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{state.job.error.message}</p>}
    {state.job.data && <EvaluationJobView job={state.job.data} actions={state.actions} />}
    {state.result.isFetching && <p role="status" className="text-xs">正在读取固定结果…</p>}
    {state.result.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{state.result.error.message}</p>}
    {state.result.data && <EvaluationResultView key={state.result.data.id} result={state.result.data} active={active} />}
  </div>
}

function EvaluationForm({ state, initialCutoff }: { state: Workspace; initialCutoff?: string }) {
  const [cutoff, setCutoff] = useState(initialCutoff || '')
  const [allowCurrent, setAllowCurrent] = useState(false)
  const actions = state.actions
  const profile = actions.pending?.profile || state.profile
  const unsupported = state.history.error instanceof ResearchApiError && state.history.error.status === 404
  const unavailable = !state.profile || Boolean(state.parameters.error) || state.parameters.isFetching || Boolean(state.history.error) || state.history.isPending
  return <section className="space-y-3" aria-label="新建历史评估">
    {state.parameters.isPending && <p role="status" className="text-xs">正在读取当前发布参数…</p>}
    {state.parameters.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">当前参数不可用：{state.parameters.error.message}</p>}
    {unsupported && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">当前研究连接尚未提供历史评估，请更新研究服务后重新读取。</p>}
    <p className="break-all text-xs text-muted-foreground">{actions.pending ? '原提交参数' : '当前发布参数'}：{profile || '尚未读取'}。新评估使用当前发布版本；已保存的报告继续保留原版本。</p>
    <form className="flex flex-wrap items-end gap-3" onSubmit={event => { event.preventDefault(); if (profile && !unavailable) void actions.act({ kind: 'create', body: { profile, cutoff, allow_current_universe: allowCurrent } }) }}>
      <label className="space-y-1 text-xs"><span className="block text-muted-foreground">评估截止美国交易日</span><input aria-label="评估截止美国交易日" type="date" required value={actions.pending?.cutoff || cutoff} disabled={actions.blocked} onChange={event => setCutoff(event.target.value)} className="h-9 rounded-lg border border-border bg-background px-3 disabled:opacity-50" /></label>
      <label className="flex max-w-md items-start gap-2 text-xs leading-relaxed"><input type="checkbox" checked={actions.pending?.allow_current_universe ?? allowCurrent} disabled={actions.blocked} onChange={event => setAllowCurrent(event.target.checked)} /><span>允许把当前股票池与分类用于历史截面（有存活偏差，不是严格时点回测）</span></label>
      <button type="submit" disabled={actions.blocked || unavailable} className={button}>{actions.busy === 'create' ? '正在提交…' : '创建历史评估'}</button>
    </form>
    <div className="flex flex-wrap gap-2">
      <button type="button" className={button} onClick={state.refresh}>重新读取评估与配置</button>
      {actions.uncertain && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'confirm' }) }}>核对原提交</button>}
      {actions.readbackId && <button type="button" className={button} disabled={actions.busy !== null} onClick={() => { void actions.act({ kind: 'read', id: actions.readbackId! }) }}>读取原任务以核对操作</button>}
    </div>
    {actions.uncertain && <p className="text-xs text-amber-700 dark:text-amber-300">提交结果尚未确认，表单已固定。核对将重用原请求，避免重复创建；请保持此页面。</p>}
    {actions.message && <p role="status" className="text-xs leading-relaxed text-amber-700 dark:text-amber-300">{actions.message}</p>}
  </section>
}

function EvaluationHistory({ state }: { state: Workspace }) {
  const history = state.history
  return <section className="space-y-3 border-t border-border pt-4" aria-label="历史评估记录"><h3 className="text-sm font-medium">评估记录</h3>
    {history.isPending && <p role="status" className="text-xs">正在读取评估记录…</p>}
    {history.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{history.error.message}</p>}
    {history.data && <><div className="grid gap-2 md:grid-cols-2">{history.data.items.map(job => <button type="button" key={job.id} aria-pressed={job.id === state.selected} className={`min-w-0 rounded-xl border p-3 text-left text-xs ${job.id === state.selected ? 'border-primary bg-primary/5' : 'border-border hover:bg-muted'}`} onClick={() => state.setSelected(job.id)}>
      <span className="block font-medium">{businessNumber(job.create_time, job.id)} · 截至 {job.cutoff} · {evaluationStatus[job.status]}</span><span className="mt-1 block break-all text-muted-foreground">{job.profile} · {job.completed_sections}/20 截面 · 创建 {new Date(job.create_time).toLocaleString('zh-CN')}</span>
    </button>)}</div>
      {history.data.total === 0 && <p className="text-xs text-muted-foreground">尚无历史评估。填写截止交易日后显式创建。</p>}
      <div className="flex items-center justify-between gap-2 text-xs"><button type="button" disabled={state.page === 1 || history.isFetching} className={button} onClick={() => state.setPage(state.page - 1)}>上一页评估</button><span>{state.page} 页 · 共 {history.data.total} 次</span><button type="button" disabled={state.page * history.data.page_size >= history.data.total || history.isFetching} className={button} onClick={() => state.setPage(state.page + 1)}>下一页评估</button></div>
    </>}
  </section>
}
