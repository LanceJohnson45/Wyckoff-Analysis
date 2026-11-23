import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { decodeResearchData } from './api'
import { watchItemSchema, watchPageSchema } from './watch-contract'
import { WatchItemView } from './watch'

const FIRST = '11111111-1111-4111-8111-111111111111'
const SECOND = '22222222-2222-4222-8222-222222222222'
const item = {
  symbol: 'AAPL', name: 'Apple', strategy: 'martin', state: 'near', target_session: '2026-11-24',
  checked_run_id: SECOND, state_run_id: FIRST, state_snapshot_id: FIRST, state_as_of: '2026-11-20', state_session: '2026-11-23',
  data_insufficient: true, not_reviewed: true, reason: 'calculation_missing', evidence_refs: ['martin/AAPL/branches/0/facts/support'], policy_version: 'watch-v1',
}

describe('持续名单的独立状态与健康标记', () => {
  it('缺少健康字段或不认识的状态拒收，不当作空名单', () => {
    expect(watchItemSchema.safeParse({ ...item, state: 'BUY' }).success).toBe(false)
    const { not_reviewed: _flag, ...missing } = item
    expect(watchItemSchema.safeParse(missing).success).toBe(false)
    expect(() => decodeResearchData({ code: '0000', message: 'ok', data: { items: [] }, correlation_id: FIRST }, watchPageSchema)).toThrow('字段不完整')
  })
  it('同时显示保留的有效状态、缺数、未复核与不同来源日期', () => {
    const html = renderToStaticMarkup(createElement(WatchItemView, { item: watchItemSchema.parse(item) }))
    expect(html).toContain('接近观察区')
    expect(html).toContain('数据不足 · 保留原状态')
    expect(html).toContain('本日未复核')
    expect(html).toContain('2026-11-23')
    expect(html).toContain('2026-11-24')
    expect(html).toContain(FIRST)
    expect(html).toContain(SECOND)
  })
  it('公司名称与引用按文本转义', () => {
    const html = renderToStaticMarkup(createElement(WatchItemView, { item: watchItemSchema.parse({ ...item, name: '<img src=x onerror=alert(1)>' }) }))
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
  })
})
