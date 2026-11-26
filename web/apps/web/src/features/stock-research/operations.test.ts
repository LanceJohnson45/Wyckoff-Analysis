import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { readOperations } from './api'
import { operationsSchema } from './operations-contract'
import { OperationsSummary } from './operations'

const ID = '11111111-1111-4111-8111-111111111111'
const stamp = '2026-11-24T21:30:00+08:00'
const phase = { state: 'not_recorded', ledger_update_time: null, has_native_receipt: false }
const value = { target_session: '2026-11-24', checked_at: stamp, latest_completed: { run_id: ID, target_session: '2026-11-23', update_time: stamp }, delivery: { target_session: '2026-11-24', checked_at: stamp, initial: phase, recovery: phase, latest_sent: null } }
const envelope = (data: unknown) => ({ code: '0000', message: 'ok', data, correlation_id: ID })
const render = (data: unknown) => renderToStaticMarkup(createElement(OperationsSummary, { value: operationsSchema.parse(data), onSelect() {} }))
afterEach(() => vi.unstubAllGlobals())

describe('操作状态契约与读取', () => {
  it('sent必须有原生回执，未知与未配置分别保留', () => {
    expect(operationsSchema.safeParse({ ...value, delivery: null }).success).toBe(true)
    expect(operationsSchema.safeParse({ ...value, delivery: { ...value.delivery, initial: { state: 'sent', ledger_update_time: stamp, has_native_receipt: false } } }).success).toBe(false)
    expect(operationsSchema.safeParse({ ...value, delivery: { ...value.delivery, initial: { state: 'unknown', ledger_update_time: stamp, has_native_receipt: false } } }).success).toBe(true)
  })
  it('无记录不能携带回执或时间，通知目标日必须与外层一致', () => {
    expect(operationsSchema.safeParse({ ...value, delivery: { ...value.delivery, initial: { ...phase, has_native_receipt: true } } }).success).toBe(false)
    expect(operationsSchema.safeParse({ ...value, delivery: { ...value.delivery, initial: { ...phase, ledger_update_time: stamp } } }).success).toBe(false)
    expect(operationsSchema.safeParse({ ...value, delivery: { ...value.delivery, target_session: '2026-11-23' } }).success).toBe(false)
  })
  it('只发目标日GET，并拒绝另一目标日响应', async () => {
    const fetch = vi.fn().mockResolvedValueOnce(Response.json(envelope(value))).mockResolvedValueOnce(Response.json(envelope({ ...value, target_session: '2026-11-23', delivery: null })))
    vi.stubGlobal('fetch', fetch)
    expect((await readOperations('2026-11-24')).latest_completed?.run_id).toBe(ID)
    expect(fetch.mock.calls[0]?.[0]).toBe('/api/stock-research/v1/operations?target_session=2026-11-24')
    expect(fetch.mock.calls[0]?.[1].method).toBeUndefined()
    await expect(readOperations('2026-11-24')).rejects.toThrow('目标日不一致')
  })
  it('连接失败不自动重试，非法目标不发出请求', async () => {
    const fetch = vi.fn().mockRejectedValue(new TypeError('offline'))
    vi.stubGlobal('fetch', fetch)
    await expect(readOperations('2026-11-24')).rejects.toThrow('未确认')
    await expect(readOperations('../private')).rejects.toThrow('日期无效')
    expect(fetch).toHaveBeenCalledTimes(1)
  })
})

describe('研究与通知的独立呈现', () => {
  it('完整研究已存在时，未知恢复仍明确未知而不是整体送达', () => {
    const html = render({ ...value, delivery: { ...value.delivery, initial: { state: 'sent', ledger_update_time: stamp, has_native_receipt: true }, recovery: { state: 'unknown', ledger_update_time: stamp, has_native_receipt: false } } })
    expect(html).toContain('最近完整研究')
    expect(html).toContain('已确认送达')
    expect(html).toContain('送达结果未知')
    expect(html).toContain('研究完成不等于通知送达')
    expect(html).not.toContain(ID)
  })
  it('无记录、未配置和无完整研究使用不同说明', () => {
    expect(render(value)).toContain('尚无记录')
    expect(render({ ...value, delivery: null })).toContain('通知状态查询未配置')
    expect(render({ ...value, latest_completed: null })).toContain('尚无完整成功研究记录')
  })
  it('最近送达与核对时间均标明账本语义，不冒充原生消息时间', () => {
    const html = render({ ...value, delivery: { ...value.delivery, latest_sent: { target_session: '2026-11-23', phase: 'recovery', ledger_update_time: stamp } } })
    expect(html).toContain('最近确认送达记录')
    expect(html).toContain('2026-11-23')
    expect(html).toContain('不代表原生消息创建时刻')
  })
})
