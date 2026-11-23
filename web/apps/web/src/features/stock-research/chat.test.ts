import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { chatTurnSchema, chatIsActive } from './chat-contract'
import { ChatTurnView } from './chat'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const ID = '11111111-1111-4111-8111-111111111111'
const REPORT = '22222222-2222-4222-8222-222222222222'
const point = { text: '固定研究观点', evidence_refs: ['market/SPY/regime'] }
const answer = { context_hash: 'a'.repeat(64), answer: point, observations: [], risks: [], confirmation_conditions: [], claims: [{ ref: 'market/SPY/regime', value: 'CRASH' }] }
const turn = { id: ID, conversation_id: ID, parent_turn_id: null, revision: 1, report_id: REPORT, question: '固定报告的问题', status: 'completed', answer, error_code: null, model: 'fixture-model', context_hash: 'a'.repeat(64), source_hash: 'b'.repeat(64), prompt_hash: 'c'.repeat(64), tool_calls: 2, receipt_available: true, create_time: '2026-11-23T12:00:00+08:00', update_time: '2026-11-23T12:01:00+08:00' }
const firstAttempt = { number: 1, status: 'failed', error_code: 'transient_provider_error', tool_calls: 1, tool_budget: 20, context_hash: 'd'.repeat(64), deadline: '2026-11-23T12:03:00+08:00', started_time: turn.create_time, finished_time: '2026-11-23T12:00:20+08:00', receipt_available: true }
const secondAttempt = { ...firstAttempt, number: 2, status: 'completed', error_code: null, tool_budget: 19, context_hash: turn.context_hash, started_time: '2026-11-23T12:00:21+08:00', finished_time: turn.update_time }

describe('固定报告私有问答', () => {
  it('拒绝伪完成、错父锚点和不同上下文的回答', () => {
    expect(chatTurnSchema.safeParse(turn).success).toBe(true)
    expect(chatTurnSchema.safeParse({ ...turn, answer: null }).success).toBe(false)
    expect(chatTurnSchema.safeParse({ ...turn, receipt_available: false }).success).toBe(false)
    expect(chatTurnSchema.safeParse({ ...turn, answer: { ...answer, context_hash: 'd'.repeat(64) } }).success).toBe(false)
    expect(chatTurnSchema.safeParse({ ...turn, revision: 2 }).success).toBe(false)
    expect(chatTurnSchema.safeParse({ ...turn, status: 'running', answer: null, error_code: 'failed' }).success).toBe(false)
  })
  it('只有已验证回答显示成功回执，外部问题和模型文字转义', () => {
    const row = chatTurnSchema.parse({ ...turn, question: '<img src=x onerror=alert(1)>', answer: { ...answer, answer: { ...point, text: '<script>bad()</script>' } } })
    const html = renderToStaticMarkup(createElement(ChatTurnView, { turn: row, disabled: false, onContinue: () => undefined }))
    expect(html).toContain('回答已保存')
    expect(html).toContain('本轮引用与数值已核验')
    expect(html).toContain('&lt;img')
    expect(html).toContain('&lt;script&gt;')
    expect(html).not.toContain('<img src=x')
    expect(html).not.toContain('<script>')
  })
  it('失败独立显示，取消和未完成不当作空回答成功', () => {
    const row = chatTurnSchema.parse({ ...turn, status: 'failed', answer: null, error_code: 'deadline_exceeded', context_hash: null, receipt_available: false })
    const html = renderToStaticMarkup(createElement(ChatTurnView, { turn: row, disabled: false, onContinue: () => undefined }))
    expect(html).toContain('本次回答未完成')
    expect(html).toContain('超过时间预算')
    expect(html).not.toContain('本轮引用与数值已核验')
    expect(chatIsActive(row)).toBe(false)
    expect(chatIsActive({ ...row, status: 'running' })).toBe(true)
  })
  it('代理只放行保存/查询/取消，不提供问答覆盖或任意执行入口', () => {
    expect(allowedResearchRoute('GET', '/v1/chat-turns')).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/chat-turns')).toBe(true)
    expect(allowedResearchRoute('GET', `/v1/chat-turns/${ID}`)).toBe(true)
    expect(allowedResearchRoute('POST', `/v1/chat-turns/${ID}/cancel`)).toBe(true)
    expect(allowedResearchRoute('GET', `/v1/chat-turns/${ID}/cancel`)).toBe(false)
    expect(allowedResearchRoute('POST', `/v1/chat-turns/${ID}/retry`)).toBe(false)
    expect(allowedResearchRoute('PUT', `/v1/chat-turns/${ID}`)).toBe(false)
    expect(allowedResearchRoute('DELETE', `/v1/chat-turns/${ID}`)).toBe(false)
  })
  it('同一回答保留两次状态及累计工具请求，旧问答不补造尝试', () => {
    const row = chatTurnSchema.parse({ ...turn, attempts: [firstAttempt, secondAttempt] })
    const html = renderToStaticMarkup(createElement(ChatTurnView, { turn: row, disabled: false, onContinue: () => undefined }))
    expect(html).toContain('执行尝试 2 / 2')
    expect(html).toContain('已确认工具请求 2 次')
    expect(html).toContain('模型服务暂不可用')
    expect(html).toContain('工具请求 1 / 19 次')
    expect(chatTurnSchema.parse(turn).attempts).toEqual([])
    const legacy = renderToStaticMarkup(createElement(ChatTurnView, { turn: chatTurnSchema.parse(turn), disabled: false, onContinue: () => undefined }))
    expect(legacy).toContain('未记录逐次执行明细')
  })
  it('拒绝重置预算、延长期限、忽略第一次请求和对非临时错误重试', () => {
    for (const invalid of [
      [firstAttempt, { ...secondAttempt, tool_budget: 20 }],
      [firstAttempt, { ...secondAttempt, deadline: '2026-11-23T12:04:00+08:00' }],
      [{ ...firstAttempt, error_code: 'invalid_output' }, secondAttempt],
      [{ ...firstAttempt, tool_calls: null }, secondAttempt],
      [firstAttempt, { ...secondAttempt, number: 1 }],
    ]) expect(chatTurnSchema.safeParse({ ...turn, attempts: invalid }).success).toBe(false)
    expect(chatTurnSchema.safeParse({ ...turn, tool_calls: 1, attempts: [firstAttempt, secondAttempt] }).success).toBe(false)
  })
  it('取消后未知调用明确待确认，先前失败与已确认调用保持', () => {
    const row = chatTurnSchema.parse({ ...turn, status: 'cancelled', answer: null, error_code: 'cancelled', tool_calls: 1, attempts: [firstAttempt, { ...secondAttempt, status: 'cancelled', error_code: 'cancelled', receipt_available: false, tool_calls: null }] })
    const html = renderToStaticMarkup(createElement(ChatTurnView, { turn: row, disabled: false, onContinue: () => undefined }))
    expect(html).toContain('调用次数待确认')
    expect(html).toContain('已确认工具请求 1 次')
    expect(html).toContain('模型服务暂不可用')
    expect(html).not.toContain('本轮引用与数值已核验')
  })
})
