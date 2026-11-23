import { useState } from 'react'
import { readManualHead } from './api'
import { canonicalResearchSymbol, type ManualHead } from './input-contract'
import type { InputActions } from './input-actions'
import { newYorkDay } from './state'

const field = 'w-full rounded-lg border border-border bg-background p-2 text-xs'
const button = 'rounded-lg border border-border px-3 py-2 text-xs disabled:opacity-40'

export function ManualInputForm({ actions }: { actions: InputActions }) {
  const [form, setForm] = useState({ symbol: '', action: 'add' as 'add' | 'remove', name: '', industry: '', source: '', reason: '', effective_date: newYorkDay() })
  const [head, setHead] = useState<ManualHead | null>(null)
  const [reading, setReading] = useState(false)
  async function readRevision() {
    setReading(true); setHead(null)
    try { setHead(await readManualHead(form.symbol)) } catch (error) { actions.showMessage(error instanceof Error ? error.message : '读取自选修订失败') }
    finally { setReading(false) }
  }
  async function publish() {
    if (!head || head.symbol !== canonicalResearchSymbol(form.symbol)) return
    await actions.submit({ kind: 'manual', key: crypto.randomUUID(), body: { symbol: head.symbol, action: form.action, name: form.action === 'add' ? form.name.trim() : null, industry: form.action === 'add' ? form.industry.trim() || null : null, source: form.source.trim(), reason: form.reason.trim(), expected_revision: head.revision, effective_date: form.effective_date } })
    setHead(null)
  }
  return <form className="space-y-3 rounded-xl border border-border p-4" aria-label="人工自选维护" onSubmit={event => { event.preventDefault(); void publish() }}><h4 className="text-sm font-medium">人工自选维护</h4><p className="text-xs text-muted-foreground">仅维护manual成员关系，移除不会删除指数成分。行业仅用于补缺；缺行情或分类仍按质量规则显示不足。</p><fieldset disabled={actions.blocked || reading} className="grid gap-3 disabled:opacity-50 md:grid-cols-2"><label className="space-y-1 text-xs">美股代码<input required maxLength={32} value={form.symbol} onChange={event => { setForm({ ...form, symbol: event.target.value }); setHead(null) }} className={field} /></label><label className="space-y-1 text-xs">动作<select value={form.action} onChange={event => setForm({ ...form, action: event.target.value === 'remove' ? 'remove' : 'add' })} className={field}><option value="add">加入或更新人工观察</option><option value="remove">移除人工成员关系</option></select></label>{form.action === 'add' && <><label className="space-y-1 text-xs">公司名称<input required maxLength={512} value={form.name} onChange={event => setForm({ ...form, name: event.target.value })} className={field} /></label><label className="space-y-1 text-xs">行业（可留空，仅补缺）<input maxLength={512} value={form.industry} onChange={event => setForm({ ...form, industry: event.target.value })} className={field} /></label></>}<label className="space-y-1 text-xs">来源引用<input required maxLength={512} value={form.source} onChange={event => setForm({ ...form, source: event.target.value })} className={field} /></label><label className="space-y-1 text-xs">生效日期（纽约）<input required type="date" min={newYorkDay()} value={form.effective_date} onChange={event => setForm({ ...form, effective_date: event.target.value })} className={field} /></label><label className="space-y-1 text-xs md:col-span-2">维护理由<textarea required maxLength={3000} rows={2} value={form.reason} onChange={event => setForm({ ...form, reason: event.target.value })} className={field} /></label><button type="button" className={button} onClick={() => { void readRevision() }}>读取当前自选修订</button><button type="submit" className={button} disabled={!head || head.symbol !== canonicalResearchSymbol(form.symbol)}>确认{form.action === 'add' ? '加入或更新' : '移除人工关系'}</button></fieldset>{head && <p className="text-xs text-muted-foreground">{head.symbol} 当前最高发布修订 {head.revision}{head.publication ? ` · 生效日 ${head.publication.effective_date}` : ' · 暂无人工发布'}。提交后须重新读取修订才能继续维护。</p>}</form>
}
