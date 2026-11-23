import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { readFrozenInputs, readManualHistory, readThemeProposals } from './api'
import { useInputActions } from './input-actions'
import { ManualInputForm } from './input-manual'
import { ThemeForm, ThemeProposalCard, ThemeReview } from './input-theme'
import type { InputPublication, ThemeProposal } from './input-contract'
import { nyTime } from './report'
import { newYorkDay } from './state'

const button = 'rounded-lg border border-border px-3 py-2 text-xs disabled:opacity-40'

export function ResearchInputMaintenance({ runId }: { runId?: string }) {
  const [open, setOpen] = useState(false)
  const [themePage, setThemePage] = useState(1)
  const [manualPage, setManualPage] = useState(1)
  const [review, setReview] = useState<ThemeProposal | null>(null)
  const actions = useInputActions(), client = useQueryClient()
  const themes = useQuery({ queryKey: ['research', 'inputs', 'proposals', themePage], queryFn: ({ signal }) => readThemeProposals(themePage, signal), enabled: open, retry: false, refetchOnWindowFocus: false })
  const manual = useQuery({ queryKey: ['research', 'inputs', 'manual', manualPage], queryFn: ({ signal }) => readManualHistory(manualPage, signal), enabled: open, retry: false, refetchOnWindowFocus: false })
  const frozen = useQuery({ queryKey: ['research', 'inputs', 'frozen', runId], queryFn: ({ signal }) => readFrozenInputs(runId!, signal), enabled: open && Boolean(runId), retry: false, refetchOnWindowFocus: false })
  return <section className="space-y-4 rounded-2xl border border-border bg-card p-5" aria-label="分组与自选维护"><div className="flex flex-wrap items-start justify-between gap-3"><div><h3 className="text-sm font-semibold">分组与自选维护</h3><p className="mt-2 text-xs text-muted-foreground">维护下一次研究的输入。主题建议须人工确认，生效日不晚于新研究截止日才可能被消费；历史报告保持原版本。</p></div><button type="button" className={button} disabled={actions.blocked} aria-expanded={open} onClick={() => setOpen(!open)}>{open ? '收起维护' : '展开维护'}</button></div><div hidden={!open} className="space-y-5">
    <button type="button" className={button} onClick={() => { void client.invalidateQueries({ queryKey: ['research', 'inputs'] }) }}>刷新维护与来源记录</button>
    {actions.message && <p role="status" className="text-xs text-amber-700 dark:text-amber-300">{actions.message}</p>}{actions.uncertain && <div className="space-y-2 rounded-lg bg-amber-500/10 p-3 text-xs"><p>原操作结果未知，已锁住新操作；使用相同输入、修订与幂等身份核对，不自动改成新发布。</p><button type="button" className={button} disabled={actions.busy} onClick={() => { void actions.submit() }}>核对原操作</button></div>}
    <details className="space-y-2 text-xs"><summary className="cursor-pointer font-medium">上方报告的冻结输入来源</summary>{runId && frozen.isPending && <p role="status">正在读取固定来源…</p>}{frozen.error && <p role="alert">{frozen.error.message}</p>}{!runId && <p>尚未选择研究报告。</p>}{frozen.data && <div className="space-y-2 break-words pt-2"><p>行情截止 {frozen.data.inputs.as_of} · {frozen.data.inputs.source === 'published' ? '已确认发布输入' : frozen.data.inputs.source === 'operator_baseline' ? 'operator基线' : '原快照版本，未补造发布来源'}</p><p>股票池 {frozen.data.inputs.universe_version}</p><p>分类 {frozen.data.inputs.classification_version}</p><p>消费人工发布 {frozen.data.inputs.publications.length} 项</p>{frozen.data.inputs.publications.map(publication => <PublicationCard key={publication.id} value={publication} />)}</div>}</details>
    <ThemeForm actions={actions} />
    <section className="space-y-3" aria-label="主题建议历史"><h4 className="text-sm font-medium">主题建议历史</h4>{themes.isPending && <p role="status" className="text-xs">正在读取建议…</p>}{themes.error && <p role="alert" className="text-xs">{themes.error.message}</p>}{themes.data && <>{themes.data.total === 0 && <p className="text-xs text-muted-foreground">暂无主题建议。</p>}{themes.data.items.map(proposal => <ThemeProposalCard key={proposal.id} proposal={proposal} disabled={actions.blocked} onReview={() => setReview(proposal)} />)}<InputPaging page={themePage} total={themes.data.total} pageSize={themes.data.page_size} busy={themes.isFetching} setPage={setThemePage} /></>}</section>
    {review && <ThemeReview key={review.id} proposal={review} actions={actions} onUpdate={setReview} onClose={() => setReview(null)} />}
    <ManualInputForm actions={actions} />
    <section className="space-y-3" aria-label="人工自选发布历史"><h4 className="text-sm font-medium">人工自选发布历史</h4>{manual.isPending && <p role="status" className="text-xs">正在读取人工修订…</p>}{manual.error && <p role="alert" className="text-xs">{manual.error.message}</p>}{manual.data && <>{manual.data.total === 0 && <p className="text-xs text-muted-foreground">暂无人工自选发布。</p>}{manual.data.items.map(publication => <PublicationCard key={publication.id} value={publication} />)}<InputPaging page={manualPage} total={manual.data.total} pageSize={manual.data.page_size} busy={manual.isFetching} setPage={setManualPage} /></>}</section>
  </div></section>
}

export function PublicationCard({ value }: { value: InputPublication }) {
  return <article className="space-y-2 rounded-lg border border-border p-3 text-xs"><p className="font-medium">{value.target} · 修订 {value.revision} · {value.kind === 'theme' ? '人工确认主题' : 'action' in value.definition && value.definition.action === 'remove' ? '移除manual关系' : '加入/更新人工观察'}</p><p className="text-muted-foreground">确认 {nyTime(value.confirmed_time)} 纽约 · 生效日 {value.effective_date}{value.effective_date > newYorkDay() ? ' · 尚未到生效日' : ' · 是否消费以研究截止日为准'}</p><p className="break-words">来源：{value.definition.source}</p><p className="whitespace-pre-wrap break-words">理由：{value.definition.reason}</p></article>
}

function InputPaging({ page, total, pageSize, busy, setPage }: { page: number; total: number; pageSize: number; busy: boolean; setPage: (page: number) => void }) {
  return <div className="flex items-center gap-4 text-xs"><span>共 {total} 项 · 第 {page} 页</span><button type="button" disabled={page === 1 || busy} onClick={() => setPage(page - 1)} className="disabled:opacity-40">上一页</button><button type="button" disabled={page * pageSize >= total || busy} onClick={() => setPage(page + 1)} className="disabled:opacity-40">下一页</button></div>
}
