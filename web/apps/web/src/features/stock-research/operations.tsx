import { useQuery } from '@tanstack/react-query'
import { readOperations, ResearchApiError } from './api'
import type { OperationsView } from './operations-contract'
import { nyTime } from './report'

const labels = { not_recorded: '尚无记录', preparing: '准备通知中', ready: '等待发送', sending: '发送处理中', retry: '等待重试', unknown: '送达结果未知', failed: '发送失败', silent: '账本记为不发送', sent: '已确认送达', unconfirmed: '送达回执尚未确认' }

export function ResearchOperations({ target, onSelect }: { target: string; onSelect: (id: string) => void }) {
  const valid = /^\d{4}-\d{2}-\d{2}$/.test(target)
  const state = useQuery({ queryKey: ['research', 'operations', target], queryFn: ({ signal }) => readOperations(target, signal), enabled: valid, retry: false, refetchOnWindowFocus: false, refetchInterval: query => !query.state.error && query.state.data?.delivery ? 30_000 : false })
  const error = state.error instanceof ResearchApiError && state.error.status === 404 ? '此研究服务尚未提供通知状态，请升级运行包。' : state.error?.message
  return <section className="space-y-3 rounded-2xl border border-border bg-card p-4" aria-label="研究与通知状态">
    <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-sm font-medium">研究与通知 · {target || '未选日期'}</h3><button type="button" disabled={!valid || state.isFetching} onClick={() => { void state.refetch() }} className="text-xs text-primary disabled:opacity-40">刷新通知与成功记录</button></div>
    {!valid && <p className="text-xs text-muted-foreground">选择目标交易日后读取操作状态。</p>}
    {valid && state.isPending && <p role="status" className="text-xs text-muted-foreground">正在核对研究与通知账本…</p>}
    {error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{error} 本次核对未成功，保留原报告供查询。</p>}
    {!error && state.data && <OperationsSummary value={state.data} onSelect={onSelect} />}
  </section>
}

export function OperationsSummary({ value, onSelect }: { value: OperationsView; onSelect: (id: string) => void }) {
  const latest = value.latest_completed
  const ledger = value.delivery
  return <div className="space-y-3 text-xs">
    {latest ? <div><p>最近完整研究 · 目标日 {latest.target_session} · 记录更新 {nyTime(latest.update_time)} 纽约</p><button type="button" onClick={() => onSelect(latest.run_id)} className="mt-1 text-primary">查看最近完整研究</button></div> : <p className="text-muted-foreground">尚无完整成功研究记录；局部报告仍可在历史研究中查看。</p>}
    {ledger ? <>
      <div className="grid gap-2 sm:grid-cols-2">{(['initial', 'recovery'] as const).map(phase => {
        const receipt = ledger[phase]
        return <div key={phase} className="rounded-xl bg-muted/30 p-3"><p>{phase === 'initial' ? '首报' : '恢复更新'} · {labels[receipt.state]}</p>{receipt.ledger_update_time && <p className="mt-1 text-muted-foreground">账本更新 {nyTime(receipt.ledger_update_time)} 纽约</p>}</div>
      })}</div>
      <p>{ledger.latest_sent ? <>最近确认送达记录 · {ledger.latest_sent.target_session} {ledger.latest_sent.phase === 'initial' ? '首报' : '恢复更新'} · {nyTime(ledger.latest_sent.ledger_update_time)} 纽约</> : '尚无带原生回执的已送达记录。'}</p>
      <p className="text-[11px] text-muted-foreground">通知账本核对 {nyTime(ledger.checked_at)} 纽约。账本更新时间不代表原生消息创建时刻。</p>
    </> : <p className="text-muted-foreground">通知状态查询未配置；请检查服务配置，不能据此判断是否已发送。</p>}
    <p className="text-[11px] leading-relaxed text-muted-foreground">通知按上方目标日读取，独立于下方历史报告修订。最近完整研究与最近送达记录覆盖全部日期；研究完成不等于通知送达。这里只核对现有记录。</p>
  </div>
}
