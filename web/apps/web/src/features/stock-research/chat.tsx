import { useEffect, useRef } from 'react'
import type { ResearchReport } from './api'
import type { ChatAnswer, ChatTurn } from './chat-contract'
import { chatIsActive } from './chat-contract'
import { useResearchChat } from './chat-state'
import { nyTime } from './report'
import { evidenceValue } from './stock'

export interface ChatSeed { report: ResearchReport; text: string; sequence: number }
const labels = { pending: '已受理', running: '正在读取证据与回答', completed: '回答已保存', failed: '本次回答未完成', cancelled: '本次问答已取消' }
const errors: Record<string, string> = { deadline_exceeded: '本次问答超过时间预算', invalid_output: '回答未通过结构或证据核验', provider_rate_limit: '模型服务限流', transient_provider_error: '模型服务暂不可用', execution_interrupted: '执行已中断，未自动重复模型请求', execution_version_changed: '执行版本已变化，请新建问题', chat_execution_failed: '本次问答执行失败，请核对研究服务配置', tool_budget_exceeded: '本轮证据调用预算已用完', boundary_or_protocol_failed: '执行器边界或协议校验未通过', cleanup_failed: '执行器收尾未确认', cancelled: '问答已取消', api_shutdown: '研究服务关闭时取消了问答', transport_error: '研究执行器暂不可用', executor_unavailable: '研究执行器不可用，请核对本机安装与权限' }
const button = 'rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted disabled:opacity-40'

export function ResearchChat({ report, currentReport, seed, bindReport, consumeSeed }: { report: ResearchReport; currentReport: ResearchReport; seed?: ChatSeed; bindReport: (report: ResearchReport) => void; consumeSeed: () => void }) {
  const state = useResearchChat(report.report_id)
  const input = useRef<HTMLTextAreaElement>(null)
  const consumed = useRef<number | null>(null)
  useEffect(() => {
    if (!seed || consumed.current === seed.sequence) return
    if (seed.report.report_id !== report.report_id) {
      if (state.busy || state.uncertain || state.running) {
        consumed.current = seed.sequence; state.showMessage('请先完成、取消或确认当前问答，再切换报告'); consumeSeed()
      } else bindReport(seed.report)
      return
    }
    consumed.current = seed.sequence
    state.loadDraft(seed.text)
    bindReport(report); consumeSeed()
    input.current?.focus({ preventScroll: true })
    input.current?.scrollIntoView({ behavior: 'smooth', block: 'center' })
  }, [seed, report, state.loadDraft, state.busy, state.uncertain, state.running, state.showMessage, bindReport, consumeSeed])
  const active = state.active.data
  const visible = state.history.data?.items || []
  return <section className="space-y-4 rounded-2xl border border-border bg-card p-5" aria-label="固定报告研究问答">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h3 className="text-sm font-semibold">固定报告研究问答</h3><p className="mt-2 text-xs text-muted-foreground">行情 {report.run.as_of} · 目标日 {report.run.target_session} · 报告修订 {report.revision}</p></div><div className="flex gap-2"><button type="button" className={button} onClick={state.refresh}>刷新问答</button><button type="button" className={button} disabled={state.busy || state.uncertain || state.running} onClick={() => state.select()}>新建会话</button></div></div>
    <p className="text-xs leading-relaxed text-muted-foreground">问题与历史固定于这份报告。后台使用只读研究工具核对证据；更换报告后另起会话。引用和数值已核验的回答仍属于研究观点。</p>
    {currentReport.report_id !== report.report_id && <div className="space-y-2 rounded-xl bg-muted/40 p-3 text-xs"><p>当前问答保留原报告修订；上方正在查看另一份报告。</p><button type="button" className={button} disabled={state.busy || state.uncertain || state.running} onClick={() => bindReport(currentReport)}>改用上方报告开始新会话</button></div>}
    {state.history.isPending && <p role="status" className="text-xs">正在读取私有问答历史…</p>}
    {state.history.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{state.history.error.message}</p>}
    {state.leaf.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">会话最新状态读取失败：{state.leaf.error.message}</p>}
    {state.active.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">问答状态读取失败：{state.active.error.message}</p>}
    <div className="space-y-4">{[...visible].reverse().map(turn => <ChatTurnView key={turn.id} turn={active?.id === turn.id ? active : turn} onContinue={() => { bindReport(report); state.select(turn) }} disabled={state.busy || state.uncertain || state.running} />)}{active && !visible.some(turn => turn.id === active.id) && <ChatTurnView turn={active} disabled onContinue={() => undefined} />}</div>
    {state.history.data && <><p className="text-[11px] text-muted-foreground">{state.conversation ? '当前会话' : '本报告全部会话'} · 共 {state.history.data.total} 问 · 第 {state.page} 页</p>{state.history.data.total === 0 && <p className="text-xs text-muted-foreground">尚无保存的问题，可从本报告或个股开始提问。</p>}<div className="flex gap-4 text-xs"><button type="button" disabled={state.page === 1} onClick={() => state.setPage(state.page - 1)} className="disabled:opacity-40">较新记录</button><button type="button" disabled={state.page * state.history.data.page_size >= state.history.data.total} onClick={() => state.setPage(state.page + 1)} className="disabled:opacity-40">较早记录</button></div></>}
    <form className="space-y-3 border-t border-border pt-4" onSubmit={event => { event.preventDefault(); bindReport(report); void state.send() }}><label className="block space-y-2 text-xs"><span>{state.conversation ? '继续当前会话' : '开始新会话'}</span><textarea ref={input} aria-label="固定报告研究问题" required maxLength={6000} value={state.draft} disabled={state.busy || state.uncertain} onChange={event => { bindReport(report); state.setDraft(event.target.value) }} rows={3} placeholder="例如：两策略对这只股票的分歧来自哪些固定证据？" className="w-full rounded-lg border border-border bg-background p-3 text-sm disabled:opacity-50" /></label><div className="flex flex-wrap items-center gap-3"><button type="submit" className={button} disabled={!state.canSend}>{state.busy ? '正在确认…' : '发送研究问题'}</button>{state.running && <><p role="status" className="text-xs text-muted-foreground">后台问答处理中，结果会保存供继续查看。</p><button type="button" className={button} disabled={state.busy} onClick={() => { void state.cancel() }}>取消本次问答</button></>}{state.uncertain && <button type="button" className={button} disabled={state.busy} onClick={() => { void state.send() }}>确认原提交</button>}<span className="text-[11px] text-muted-foreground">{state.draft.length} / 6000</span></div></form>
    {state.message && <p role="status" className="text-xs text-amber-700 dark:text-amber-300">{state.message}</p>}
  </section>
}

export function ChatTurnView({ turn, onContinue, disabled }: { turn: ChatTurn; onContinue: () => void; disabled: boolean }) {
  return <article className="space-y-3 rounded-xl border border-border p-4">
    <p className="whitespace-pre-wrap break-words text-sm font-medium">{turn.question}</p><p className="text-[11px] text-muted-foreground">会话第 {turn.revision} 问 · {nyTime(turn.create_time)} 纽约 · {labels[turn.status]}{turn.model ? ` · ${turn.model}` : ''}</p>
    {turn.answer && <ChatAnswerView answer={turn.answer} />}
    {turn.error_code && <p className="text-xs text-amber-700 dark:text-amber-300">{errors[turn.error_code] || '本次执行未确认有效回答，请查看保存状态'}</p>}
    {turn.attempts.length > 0 ? <details className="text-xs"><summary className="cursor-pointer text-muted-foreground">执行尝试 {turn.attempts.length} / 2 · 已确认工具请求 {turn.tool_calls} 次</summary><ol className="mt-2 space-y-2">{turn.attempts.map(attempt => <li key={attempt.number} className="rounded-lg bg-muted/40 p-3"><p>第 {attempt.number} 次 · {labels[attempt.status]} · {attempt.tool_calls === null ? '调用次数待确认' : `工具请求 ${attempt.tool_calls} / ${attempt.tool_budget} 次`}</p>{attempt.error_code && <p className="mt-1">{errors[attempt.error_code] || '本次尝试未完成'}</p>}<p className="mt-1 text-muted-foreground">{nyTime(attempt.started_time)}{attempt.receipt_available ? ' · 回执已保存' : ''}</p></li>)}</ol></details> : <p className="text-[11px] text-muted-foreground">未记录逐次执行明细</p>}
    {turn.status === 'completed' && <p className="text-[11px] text-muted-foreground">本轮引用与数值已核验 · 工具调用 {turn.tool_calls} 次 · 回执已保存</p>}
    <button type="button" className={button} disabled={disabled} onClick={onContinue}>{chatIsActive(turn) ? '查看此会话进度' : '继续此会话'}</button>
  </article>
}

export function ChatAnswerView({ answer }: { answer: ChatAnswer }) {
  return <div className="space-y-3 text-sm"><p className="whitespace-pre-wrap leading-relaxed">{answer.answer.text}</p>{[{ title: '证据观察', items: answer.observations }, { title: '风险与限制', items: answer.risks }, { title: '待确认条件', items: answer.confirmation_conditions }].map(group => group.items.length > 0 && <div key={group.title}><h4 className="text-xs font-medium text-muted-foreground">{group.title}</h4><ul className="mt-2 space-y-2 pl-4 text-xs leading-relaxed">{group.items.map((item, index) => <li key={index} className="list-disc whitespace-pre-wrap">{item.text}</li>)}</ul></div>)}
    <details className="text-xs"><summary className="cursor-pointer text-muted-foreground">引用与原始数值</summary><div className="mt-3 max-h-72 space-y-3 overflow-auto">{answer.claims.map((claim, index) => <div key={index} className="rounded-lg bg-muted/40 p-3"><p className="break-all font-mono text-[10px]">{claim.ref}</p><p className="mt-1 whitespace-pre-wrap break-words">{evidenceValue(claim.value)}</p></div>)}{[answer.answer, ...answer.observations, ...answer.risks, ...answer.confirmation_conditions].map((point, index) => <div key={index} className="space-y-1"><p>{point.text}</p>{point.evidence_refs.map(ref => <code key={ref} className="block break-all text-[10px] text-muted-foreground">{ref}</code>)}</div>)}</div></details>
  </div>
}
