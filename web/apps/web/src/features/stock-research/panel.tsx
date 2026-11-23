import { useRef, useState } from 'react'
import { useStockResearch, isRunning } from './state'
import { ResearchHistory } from './history'
import { ResearchReportView } from './report'
import { ResearchWatchlists } from './watch'
import { ResearchStockPanel } from './stock'
import { ResearchParameters } from './parameters'
import { ResearchPreparations } from './preparations'
import { ResearchChat, type ChatSeed } from './chat'
import type { ResearchReport } from './api'
import { ResearchInputMaintenance } from './inputs'

export default function StockResearchPanel() {
  const state = useStockResearch()
  const report = state.report.data
  const [seed, setSeed] = useState<ChatSeed>()
  const [chatReport, setChatReport] = useState<ResearchReport | null>(null)
  const activeChatReport = chatReport || report
  const seedSequence = useRef(0)
  const discussStock = (text: string) => { if (report) { seedSequence.current += 1; setSeed({ report, text, sequence: seedSequence.current }) } }
  const discuss = () => {
    if (!report) return
    discussStock('请基于本报告说明两策略的初选、分歧、风险和待确认条件，并给出实际证据引用。')
  }
  return <div className="mx-auto w-full max-w-7xl space-y-5" aria-label="股票策略研究">
    <div className="flex flex-wrap items-center justify-between gap-3"><div><h2 className="text-lg font-semibold">策略研究</h2><p className="mt-1 text-xs text-muted-foreground">独立初选 · 交叉评议 · 固定快照</p></div><button type="button" onClick={state.refresh} className="rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted">刷新报告与运行记录</button></div>
    <ResearchControls state={state} />
    <ResearchPreparations key={state.actions.target} target={state.actions.target} preparing={state.actions.busy === 'create'} onSelect={state.selectRun} />
    {state.fixed && <p className="rounded-xl bg-muted p-3 text-xs">正在查看固定历史修订；其状态保持当时原样。选择“查看实时状态”可返回本次运行。</p>}
    {state.connectionProblem && <p role="status" className="rounded-xl bg-amber-500/10 p-3 text-xs text-amber-700 dark:text-amber-300">{state.connectionProblem}</p>}
    <div className="grid items-start gap-5 lg:grid-cols-[240px_minmax(0,1fr)]"><ResearchHistory state={state} /><section className="min-w-0" aria-label="研究报告内容">
      {state.report.isPending && <p role="status" className="rounded-2xl border border-border p-8 text-sm text-muted-foreground">正在读取固定研究报告…</p>}
      {state.report.error && <div role="alert" className="rounded-2xl border border-amber-500/30 bg-amber-500/5 p-6"><h3 className="text-sm font-semibold">研究连接暂不可用</h3><p className="mt-2 text-sm text-muted-foreground">{state.report.error.message}</p><button type="button" onClick={state.refresh} className="mt-4 rounded-lg border border-border px-3 py-2 text-xs">重新连接</button></div>}
      {!state.report.isPending && !state.report.error && !report && <p role="status" className="rounded-2xl border border-border p-8 text-sm text-muted-foreground">{state.active ? '研究已受理，正在固定输入并准备首份报告…' : '暂无研究报告。可选择目标交易日运行预览。'}</p>}
      {report && <div className="space-y-5"><ResearchReportView report={report} onDiscuss={discuss} /><ResearchStockPanel key={report.report_id} report={report} onDiscuss={discussStock} /></div>}
      {activeChatReport && <div className="mt-5"><ResearchChat key={`chat:${activeChatReport.report_id}`} report={activeChatReport} currentReport={report || activeChatReport} seed={seed} bindReport={setChatReport} consumeSeed={() => setSeed(undefined)} /></div>}
    </section></div>
    <ResearchWatchlists />
    <ResearchParameters runId={report?.run.run_id} />
    <ResearchInputMaintenance runId={report?.run.run_id} />
  </div>
}

function ResearchControls({ state }: { state: ReturnType<typeof useStockResearch> }) {
  const run = state.report.data?.run
  const disabled = state.actions.busy !== null
  return <section className="rounded-2xl border border-border bg-card p-4" aria-label="研究运行操作"><form className="flex flex-wrap items-end gap-3" onSubmit={(event) => { event.preventDefault(); void state.actions.act('create') }}>
    <label className="space-y-1 text-xs"><span className="block text-muted-foreground">目标美国交易日</span><input aria-label="目标美国交易日" type="date" required value={state.actions.target} onChange={(event) => state.actions.setTarget(event.target.value)} className="h-9 rounded-lg border border-border bg-background px-3" /></label>
    <button type="submit" disabled={disabled} className="h-9 rounded-lg bg-primary px-4 text-xs font-medium text-primary-foreground disabled:opacity-40">{state.actions.busy === 'create' ? '正在固定研究输入…' : '运行该日预览'}</button>
    {run && <button type="button" disabled={disabled} onClick={() => { void state.actions.act('create', true) }} className="h-9 rounded-lg border border-border px-3 text-xs disabled:opacity-40">重新分析本报告</button>}
    {run && !state.fixed && ['partial', 'failed', 'cancelled'].includes(run.status) && <button type="button" disabled={disabled} onClick={() => { void state.actions.act('retry') }} className="h-9 rounded-lg border border-border px-3 text-xs disabled:opacity-40">仅续跑未成功阶段</button>}
    {run && !state.fixed && isRunning(run.status) && <button type="button" disabled={disabled} onClick={() => { void state.actions.act('cancel') }} className="h-9 rounded-lg border border-border px-3 text-xs disabled:opacity-40">取消本次运行</button>}
  </form><p className="mt-3 text-xs text-muted-foreground">预览会保存研究与模型结论，不发送通知。重新分析创建新版本，原初选继续可查。</p>{state.actions.error && <p role="alert" className="mt-3 text-xs text-amber-700 dark:text-amber-300">{state.actions.error}</p>}</section>
}
