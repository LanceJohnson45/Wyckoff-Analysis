import { evaluationActive, type EvaluationJob } from './evaluation-contract'
import type { useEvaluationWorkspace } from './evaluation-state'

export const evaluationStatus: Record<EvaluationJob['status'], string> = { pending: '待运行', running: '运行中', completed: '已完成', failed: '失败', cancelled: '已取消' }
const dataFailures = new Map([
  ['evaluation_history_insufficient', '完整历史截面不足20个。请补齐三基准和股票历史后创建新评估；原输入续跑不能补充资料。'],
  ['evaluation_quality_failed', '历史截面行情质量未通过。请核查缺数、覆盖和来源，补齐资料后创建新评估。'],
])

export function EvaluationJobView({ job, actions }: { job: EvaluationJob; actions: ReturnType<typeof useEvaluationWorkspace>['actions'] }) {
  const active = evaluationActive(job)
  const dataFailure = job.error_code ? dataFailures.get(job.error_code) : undefined
  return <section className="space-y-3 rounded-xl border border-border p-4" aria-label="所选评估任务">
    <h3 className="text-sm font-medium">截至 {job.cutoff} · {evaluationStatus[job.status]}</h3>
    <p className="break-all text-xs text-muted-foreground">参数 {job.profile} · 已完成 {job.completed_sections}/20 截面 · 尝试 {job.attempt_count}/2</p>
    <progress aria-label="评估截面进度" value={job.completed_sections} max={20} className="h-2 w-full accent-primary" />
    {job.allow_current_universe && <p className="text-xs text-amber-700 dark:text-amber-300">本任务明确允许当前成分与分类回填，有存活偏差。</p>}
    {job.cancel_requested && active && <p role="status" className="text-xs">已请求取消，正在等待计算进程实际退出。</p>}
    {job.error_code && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{dataFailure || `任务未完成：${job.error_code}。已完成截面继续保留。`}</p>}
    <div className="flex flex-wrap gap-2">
      {active && !job.cancel_requested && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'cancel', id: job.id }) }} className="rounded-lg border border-border px-3 py-2 text-xs disabled:opacity-40">取消所选评估</button>}
      {['failed', 'cancelled'].includes(job.status) && job.attempt_count < 2 && !dataFailure && <button type="button" disabled={actions.blocked} onClick={() => { void actions.act({ kind: 'retry', id: job.id }) }} className="rounded-lg border border-border px-3 py-2 text-xs disabled:opacity-40">按原输入续跑</button>}
    </div>
    {['failed', 'cancelled'].includes(job.status) && !dataFailure && <p className="text-xs text-muted-foreground">续跑保留原输入、已完成截面和原截止期限；次数或期限用尽后服务会明确拒绝。</p>}
    <details><summary className="cursor-pointer text-xs text-muted-foreground">核对任务身份</summary><p className="mt-2 break-all text-[11px]">{job.id}</p></details>
  </section>
}
