import type { useStockResearch } from './state'
import { statusLabels } from './state'
import { nyTime } from './report'

type State = ReturnType<typeof useStockResearch>

export function ResearchHistory({ state }: { state: State }) {
  return <aside className="space-y-4 rounded-2xl border border-border bg-card p-4" aria-label="历史研究与修订">
    <div className="flex items-center justify-between"><h3 className="text-sm font-semibold">历史研究</h3><button type="button" className="text-xs text-muted-foreground hover:text-foreground" onClick={() => state.selectRun(null)}>最近报告</button></div>
    {state.runs.isPending && <p className="text-xs text-muted-foreground">正在读取运行记录…</p>}
    {state.runs.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{state.runs.error.message}</p>}
    {state.runs.data && <><div className="space-y-2">{state.runs.data.items.map((run) => <button type="button" key={run.run_id} aria-pressed={state.runId === run.run_id} onClick={() => state.selectRun(run.run_id)} className="flex w-full items-center justify-between gap-2 rounded-lg border border-border px-3 py-2 text-left text-xs hover:bg-muted aria-pressed:bg-primary/10"><span>{run.target_session}<small className="mt-1 block text-muted-foreground">{nyTime(run.create_time)} 纽约</small></span><span>{statusLabels[run.status]}</span></button>)}</div>
      {state.runs.data.total === 0 && <p className="text-xs text-muted-foreground">暂无已保存的研究运行</p>}
      <div className="flex items-center justify-between text-xs"><button type="button" disabled={state.page === 1} onClick={() => state.setPage(state.page - 1)} className="disabled:opacity-40">上一页</button><span>{state.page} 页 · 共 {state.runs.data.total} 次</span><button type="button" disabled={state.page * state.runs.data.page_size >= state.runs.data.total} onClick={() => state.setPage(state.page + 1)} className="disabled:opacity-40">下一页</button></div>
    </>}
    {state.runId && <div className="space-y-2 border-t border-border pt-4"><h4 className="text-xs font-semibold">该次研究的固定修订</h4>
      <button type="button" onClick={() => { if (state.runId) state.selectRun(state.runId) }} className="text-xs text-primary">查看实时状态</button>
      {state.revisions.isPending && <p className="text-xs text-muted-foreground">正在读取修订…</p>}
      {state.revisions.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{state.revisions.error.message}</p>}
      {state.revisions.data && <div className="max-h-64 space-y-1 overflow-auto">{state.revisions.data.items.map((revision) => <button type="button" key={revision.report_id} onClick={() => state.selectRevision(revision.report_id)} aria-pressed={state.fixed && state.report.data?.report_id === revision.report_id} className="flex w-full justify-between rounded-lg px-3 py-2 text-xs hover:bg-muted aria-pressed:bg-primary/10"><span>修订 {revision.revision} · {nyTime(revision.create_time)}</span><span>{statusLabels[revision.status]}</span></button>)}</div>}
    </div>}
  </aside>
}
