import { useState } from 'react'
import { readThemeProposal } from './api'
import { canonicalResearchSymbol, themeDefinitionSchema, type ThemeProposal } from './input-contract'
import type { InputActions } from './input-actions'
import { newYorkDay } from './state'
import { nyTime } from './report'

const field = 'w-full rounded-lg border border-border bg-background p-2 text-xs'
const button = 'rounded-lg border border-border px-3 py-2 text-xs disabled:opacity-40'

export function ThemeForm({ actions }: { actions: InputActions }) {
  const [form, setForm] = useState({ name: '', members: '', source: '', reason: '' })
  const [error, setError] = useState('')
  async function propose() {
    setError('')
    try {
      const members = form.members.trim() ? form.members.trim().split(/[\s,，]+/).map(canonicalResearchSymbol) : []
      if (new Set(members).size !== members.length) throw new Error('规范化后的主题成员重复，请先修正')
      const body = themeDefinitionSchema.parse({ ...form, members: members.sort() })
      if (await actions.submit({ kind: 'proposal', body, key: crypto.randomUUID() })) setForm({ ...form, name: '', members: '', reason: '' })
    } catch { setError('请填写主题、来源和理由；成员最多2000只且须为不同的规范美股代码。空成员表示提议清空该主题。') }
  }
  return <form className="space-y-3 rounded-xl border border-border p-4" onSubmit={event => { event.preventDefault(); void propose() }}><h4 className="text-sm font-medium">提交主题建议</h4><p className="text-xs text-muted-foreground">成员为该主题的完整名单，留空表示清空；提交建议后还需人工复核确认。</p><fieldset disabled={actions.blocked} className="grid gap-3 disabled:opacity-50 md:grid-cols-2"><label className="space-y-1 text-xs">主题名称<input required maxLength={128} className={field} value={form.name} onChange={event => setForm({ ...form, name: event.target.value })} /></label><label className="space-y-1 text-xs">来源引用<input required maxLength={512} className={field} value={form.source} onChange={event => setForm({ ...form, source: event.target.value })} /></label><label className="space-y-1 text-xs md:col-span-2">完整成员代码<textarea rows={3} className={field} value={form.members} onChange={event => setForm({ ...form, members: event.target.value })} placeholder="如 AAPL, MSFT；逗号或空格分隔" /></label><label className="space-y-1 text-xs md:col-span-2">提议理由<textarea required maxLength={3000} rows={2} className={field} value={form.reason} onChange={event => setForm({ ...form, reason: event.target.value })} /></label><button className={button} type="submit">保存待确认建议</button></fieldset>{error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{error}</p>}</form>
}

export function ThemeProposalCard({ proposal, onReview, disabled }: { proposal: ThemeProposal; onReview: () => void; disabled: boolean }) {
  const stale = proposal.base_revision !== proposal.current_revision
  return <article className="space-y-2 rounded-xl border border-border p-3 text-xs"><p className="font-medium">{proposal.definition.name} · {proposal.definition.members.length} 只 · {proposal.status === 'confirmed' ? '已人工确认' : stale ? '基准已变化，须重新提议' : '待人工确认'}</p><p className="text-muted-foreground">提议 {nyTime(proposal.create_time)} 纽约 · 基准修订 {proposal.base_revision} · 当前发布修订 {proposal.current_revision}</p><p className="break-words text-muted-foreground">来源：{proposal.definition.source}</p>{proposal.publication && <p>确认修订 {proposal.publication.revision} · 生效日 {proposal.publication.effective_date}</p>}<button type="button" className={button} disabled={disabled} onClick={onReview}>查看完整成员与确认</button></article>
}

export function ThemeReview({ proposal, actions, onUpdate, onClose }: { proposal: ThemeProposal; actions: InputActions; onUpdate: (proposal: ThemeProposal) => void; onClose: () => void }) {
  const [effective, setEffective] = useState(newYorkDay)
  const [reading, setReading] = useState(false)
  const previous = new Set(proposal.previous_members), next = new Set(proposal.definition.members)
  const added = proposal.definition.members.filter(symbol => !previous.has(symbol))
  const removed = proposal.previous_members.filter(symbol => !next.has(symbol))
  const stale = proposal.status === 'pending' && proposal.current_revision !== proposal.base_revision
  async function refresh() { setReading(true); try { onUpdate(await readThemeProposal(proposal.id)) } catch (error) { actions.showMessage(error instanceof Error ? error.message : '提议刷新失败') } finally { setReading(false) } }
  async function confirm() {
    await actions.submit({ kind: 'confirm', identifier: proposal.id, body: { expected_revision: proposal.base_revision, effective_date: effective } })
    await refresh()
  }
  return <section className="space-y-3 rounded-xl border border-amber-500/40 bg-amber-500/5 p-4 text-xs" aria-label="人工复核主题"><h4 className="text-sm font-medium">人工复核：{proposal.definition.name}</h4><p className="whitespace-pre-wrap">理由：{proposal.definition.reason}</p><p className="break-words">来源：{proposal.definition.source}</p><div className="max-h-56 space-y-2 overflow-auto break-words"><p>原完整成员：{proposal.previous_members.join(', ') || '空'}</p><p>新完整成员：{proposal.definition.members.join(', ') || '空'}</p><p>加入：{added.join(', ') || '无'}</p><p>移除：{removed.join(', ') || '无'}</p></div><p>确认使用提议基准修订 {proposal.base_revision}；当前修订 {proposal.current_revision}。确认不改历史报告。</p>{stale && <p className="text-amber-700 dark:text-amber-300">已有新发布，当前建议不能覆盖。请根据最新成员提交新的建议。</p>}{proposal.publication ? <p>已确认修订 {proposal.publication.revision}，生效日 {proposal.publication.effective_date}。</p> : <form className="flex flex-wrap items-end gap-3" onSubmit={event => { event.preventDefault(); void confirm() }}><label className="space-y-1">生效日期（纽约）<input required type="date" min={newYorkDay()} value={effective} disabled={actions.blocked || reading} onChange={event => setEffective(event.target.value)} className={field} /></label><button type="submit" className={button} disabled={actions.blocked || reading || stale}>确认并发布此主题</button></form>}<div className="flex gap-3"><button type="button" className={button} disabled={actions.blocked || reading} onClick={() => { void refresh() }}>读取最新确认状态</button><button type="button" className={button} disabled={actions.blocked || reading} onClick={onClose}>关闭复核</button></div></section>
}
