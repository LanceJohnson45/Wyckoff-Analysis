import { quantFailureLabel, quantStatusLabels } from './quant-contract'
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { readQuantAttempts } from './api'
import { strategyLabels } from './state'

export function QuantAttemptHistory({ runId }: { runId: string }) {
  const [open, setOpen] = useState(false)
  return <section className="mt-4 border-t border-border pt-3 text-xs"><button type="button" aria-expanded={open} onClick={() => setOpen(!open)} className="underline">本运行当前量化尝试历史</button><p className="mt-2 text-muted-foreground">当前尝试历史独立于上方报告修订，补算记录不改写历史结论。</p>{open && <QuantAttempts key={runId} runId={runId} />}</section>
}

function QuantAttempts({ runId }: { runId: string }) {
  const [page, setPage] = useState(1)
  const query = useQuery({ queryKey: ['research', 'quant-attempts', runId, page], queryFn: ({ signal }) => readQuantAttempts(runId, page, signal), retry: false, refetchOnWindowFocus: false })
  return <div className="mt-3 space-y-3">
    {query.isPending && <p role="status">正在读取量化尝试…</p>}
    {query.error && <p role="alert" className="text-amber-700 dark:text-amber-300">{query.error.message}<button type="button" onClick={() => { void query.refetch() }} className="ml-2 underline">重试</button></p>}
    {query.data && <>{!query.data.recorded ? <p className="text-muted-foreground">旧运行未单独记录量化尝试，不能据此推断从未失败。</p> : query.data.items.length === 0 ? <p className="text-muted-foreground">此页尚无已登记尝试。</p> : <div className="space-y-2">{query.data.items.map(row => <div key={row.id} className="rounded-lg border border-border p-3"><p>{strategyLabels[row.strategy]} · 第 {row.attempt_count} 次 · {quantStatusLabels[row.status]}</p>{row.error_code && <p className="mt-1 text-amber-700 dark:text-amber-300">{quantFailureLabel(row.error_code) || '量化执行未完成，请查看运行状态'}</p>}</div>)}</div>}<div className="flex gap-4"><button type="button" disabled={page === 1 || query.isFetching} onClick={() => setPage(page-1)} className="disabled:opacity-40">上一页</button><button type="button" disabled={page*query.data.page_size >= query.data.total || query.isFetching} onClick={() => setPage(page+1)} className="disabled:opacity-40">下一页</button></div></>}
  </div>
}
