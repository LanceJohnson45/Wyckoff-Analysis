import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { preparationSchema } from './preparation-contract'
import { PreparationItemView } from './preparations'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const ID = '11111111-1111-4111-8111-111111111111'
const receipt = { id: ID, market: 'us', target_session: '2026-11-23', as_of: '2026-11-20', origin: 'api', request_id: ID, status: 'unconfirmed', phase: 'snapshot', quality: null, error_code: null, run_id: null, source_hash: 'a'.repeat(64), create_time: '2026-11-23T12:00:00+08:00', update_time: '2026-11-23T12:00:00+08:00' }
const refresh = { id: ID, as_of: receipt.as_of, source_hash: receipt.source_hash, status: 'unconfirmed', started_at: receipt.create_time, finished_at: null, sync: null, benchmark_hash: null, error_code: null }
const synced = { market: 'us', rows: 1280, batches: 1, rejected: 0, issues: 1, checkpoint: receipt.create_time, scope: 'research:us:all:benchmarks-v1' }

describe('输入准备回执', () => {
  it('未确认不画为失败或空选股，缺质量不补零', () => {
    const html = renderToStaticMarkup(createElement(PreparationItemView, { item: preparationSchema.parse(receipt), onSelect: () => undefined }))
    expect(html).toContain('准备尚未确认')
    expect(html).toContain('尚无可确认的快照质量数据')
    expect(html).not.toContain('输入准备失败')
    expect(html).not.toContain('覆盖 0')
    expect(html).not.toContain('查看对应研究')
  })
  it('拒绝缺运行、缺质量或错请求引用的伪完成回执', () => {
    expect(preparationSchema.safeParse({ ...receipt, status: 'ready' }).success).toBe(false)
    expect(preparationSchema.safeParse({ ...receipt, status: 'ready', run_id: ID, phase: 'freeze' }).success).toBe(false)
    expect(preparationSchema.safeParse({ ...receipt, status: 'failed' }).success).toBe(false)
    expect(preparationSchema.safeParse({ ...receipt, origin: 'cli' }).success).toBe(false)
  })
  it('失败显示真实覆盖与限制，外部文字保持转义', () => {
    const item = preparationSchema.parse({ ...receipt, status: 'failed', error_code: 'quality_failed', quality: { passed: false, coverage: 0.5, aligned_count: 1, universe_count: 2, issues: [{ symbol: 'AAPL', strategy: 'all', reason: '<img src=x onerror=alert(1)>' }], limitations: [] } })
    const html = renderToStaticMarkup(createElement(PreparationItemView, { item, onSelect: () => undefined }))
    expect(html).toContain('输入准备失败')
    expect(html).toContain('50.00%')
    expect(html).toContain('1/2 只对齐')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
    expect(allowedResearchRoute('GET', '/v1/preparations')).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/preparations')).toBe(false)
  })
  it('尚未确认的更新不补造同步行数或成功', () => {
    const item = preparationSchema.parse({ ...receipt, market_refresh: refresh })
    const html = renderToStaticMarkup(createElement(PreparationItemView, { item, onSelect: () => undefined }))
    expect(html).toContain('更新尚未确认')
    expect(html).toContain('尚无可确认的同步结果')
    expect(html).not.toContain('0 行已同步')
  })
  it('基准失败仍显示已确认同步，保持快照与研究状态分开', () => {
    const item = preparationSchema.parse({ ...receipt, market_refresh: { ...refresh, status: 'failed', sync: synced, error_code: 'refresh_benchmark_failed', finished_at: receipt.update_time } })
    const html = renderToStaticMarkup(createElement(PreparationItemView, { item, onSelect: () => undefined }))
    expect(html).toContain('基准补充失败')
    expect(html).toContain('1280 行已同步')
    expect(html).toContain('1 项隔离待核对')
    expect(html).toContain('尚无可确认的快照质量数据')
    expect(html).not.toContain('查看对应研究')
  })
  it('拒收缺同步的伪完成或其他准备的更新，不给旧记录制造更新历史', () => {
    expect(preparationSchema.parse(receipt).market_refresh).toBeNull()
    expect(preparationSchema.safeParse({ ...receipt, market_refresh: { ...refresh, status: 'completed', finished_at: receipt.update_time } }).success).toBe(false)
    expect(preparationSchema.safeParse({ ...receipt, market_refresh: { ...refresh, as_of: '2026-11-19' } }).success).toBe(false)
    expect(preparationSchema.safeParse({ ...receipt, market_refresh: { ...refresh, source_hash: 'b'.repeat(64) } }).success).toBe(false)
  })
})
