import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { readPreparations } from './api'
import type { PreparationAttempt } from './preparation-contract'
import { nyTime } from './report'

const phases = { parameters: '参数解析', snapshot: '行情快照', analysis: '策略量化', freeze: '冻结输入' }
const statuses = { unconfirmed: '准备尚未确认', ready: '输入已冻结', failed: '输入准备失败', cancelled: '输入准备已取消' }
const errors = { quality_failed: '行情覆盖或基准未通过质量门槛', input_unavailable: '输入或执行配置暂不可用', source_changed: '代码版本已变化，须重启研究服务', cancelled: '准备过程已取消', incomplete: '未确认冻结结果' }

export function ResearchPreparations({ target, preparing, onSelect }: { target: string; preparing: boolean; onSelect: (id: string) => void }) {
  const [page, setPage] = useState(1)
  const valid = /^\d{4}-\d{2}-\d{2}$/.test(target)
  const attempts = useQuery({ queryKey: ['research', 'preparations', target, page], queryFn: ({ signal }) => readPreparations(target, page, signal), enabled: valid, retry: false, refetchOnWindowFocus: false, refetchInterval: preparing ? 3000 : false })
  return <section className="space-y-3 rounded-2xl border border-border bg-card p-4" aria-label="研究输入准备回执">
    <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-sm font-medium">输入准备 · {target || '未选日期'}</h3><button type="button" disabled={!valid || attempts.isFetching} onClick={() => { void attempts.refetch() }} className="text-xs text-primary disabled:opacity-40">刷新准备记录</button></div>
    {!valid && <p className="text-xs text-muted-foreground">选择目标交易日后查询准备记录。</p>}
    {valid && attempts.isPending && <p role="status" className="text-xs text-muted-foreground">正在读取该日准备回执…</p>}
    {attempts.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{attempts.error.message}</p>}
    {attempts.data && <>{attempts.data.items.map(item => <PreparationItemView key={item.id} item={item} onSelect={onSelect} />)}
      {attempts.data.total === 0 && <p className="text-xs text-muted-foreground">该日尚无准备回执。历史运行不补造记录；空历史不表示行情已检查。</p>}
      <div className="flex justify-between gap-2 text-xs text-muted-foreground"><button type="button" disabled={page === 1} onClick={() => setPage(page - 1)} className="disabled:opacity-40">上一页</button><span>{page} 页 · 共 {attempts.data.total} 次</span><button type="button" disabled={page * attempts.data.page_size >= attempts.data.total} onClick={() => setPage(page + 1)} className="disabled:opacity-40">下一页</button></div>
    </>}
    <p className="text-[11px] leading-relaxed text-muted-foreground">此处按运行操作中所选目标日查询，与下方历史报告分别展示。准备失败不清空旧报告或名单；未确认可能仍在处理，也可能已中断，不能视为成功或失败。</p>
  </section>
}

export function PreparationItemView({ item, onSelect }: { item: PreparationAttempt; onSelect: (id: string) => void }) {
  return <article className="space-y-2 rounded-xl bg-muted/30 p-3">
    <p className="text-xs font-medium">{statuses[item.status]} · {phases[item.phase]}<span className="ml-2 text-muted-foreground">{item.origin === 'api' ? '工作台/API' : 'CLI'}</span></p>
    <p className="text-[11px] text-muted-foreground">目标 {item.target_session} · 要求行情截止 {item.as_of} · {nyTime(item.update_time)} 纽约</p>
    {item.error_code && <p className="text-xs text-amber-700 dark:text-amber-300">{errors[item.error_code]}</p>}
    {item.quality ? <p className="text-xs">快照质量{item.quality.passed ? '通过' : '未通过'} · 覆盖 {(item.quality.coverage * 100).toFixed(2)}% · {item.quality.aligned_count}/{item.quality.universe_count} 只对齐</p> : <p className="text-xs text-muted-foreground">尚无可确认的快照质量数据</p>}
    {item.quality && <details><summary className="cursor-pointer text-[11px] text-muted-foreground">质量问题与限制 · {item.quality.issues.length} 项</summary><div className="mt-2 max-h-48 space-y-1 overflow-auto text-[11px]">{item.quality.issues.map((issue, index) => <p key={`${issue.symbol}:${issue.reason}:${index}`}>{issue.symbol} · {issue.strategy} · {issue.reason}</p>)}{item.quality.limitations.map((text, index) => <p key={index}>{text}</p>)}</div></details>}
    {item.run_id && <><p className="text-[11px] text-muted-foreground">已冻结输入不代表模型研究已完成。</p><button type="button" className="text-xs text-primary" onClick={() => { if (item.run_id) onSelect(item.run_id) }}>查看对应研究</button></>}
  </article>
}
