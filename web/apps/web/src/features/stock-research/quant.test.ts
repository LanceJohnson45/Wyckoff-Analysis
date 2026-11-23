import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { quantJobSchema, quantAttemptPageSchema } from './quant-contract'
import { reportSchema, readQuantAttempts } from './api'
import { ResearchReportView } from './report'
import { stockObservationSchema } from './stock-contract'
import { StockFactsView } from './stock'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const A = '11111111-1111-4111-8111-111111111111', B = '22222222-2222-4222-8222-222222222222'
const failed = { id: A, strategy: 'martin' as const, status: 'failed' as const, attempt_count: 1, artifact_id: null, analysis_hash: null, error_code: 'quant_calculation_failed' }
const success = { id: B, strategy: 'wyckoff' as const, status: 'completed' as const, attempt_count: 1, artifact_id: B, analysis_hash: 'b'.repeat(64), error_code: null }
afterEach(() => vi.unstubAllGlobals())

function partialReport() {
  return reportSchema.parse({ report_id: A, revision: 3, create_time: '2026-11-23T08:30:00-05:00', content_hash: 'a'.repeat(64), run: {
    run_id: B, market: 'us', target_session: '2026-11-23', as_of: '2026-11-20', snapshot_id: A, snapshot_hash: 'a'.repeat(64), report_due: '2026-11-23T08:30:00-05:00', observed_at: '2026-11-23T08:30:00-05:00', late: false, preview: true, status: 'partial', error_code: 'quant_unavailable', companies: [],
    quality: { passed: true, coverage: 1, aligned_count: 1, universe_count: 1, issues: [], limitations: [] }, quant: [failed, success], stages: [], reviews: [], selections: [{ id: B, strategy: 'wyckoff', source: 'empty_pool', decision: { role: 'select', strategy: 'wyckoff', context_hash: 'b'.repeat(64), items: [], empty_reason: { text: '原合格池为空', evidence_refs: ['scope/pool_size'] } } }],
  } })
}

describe('量化局部失败与独立尝试历史', () => {
  it('失败、未完成和旧运行回执不能冒充成功产物或虚构尝试次数', () => {
    expect(quantJobSchema.safeParse(failed).success).toBe(true)
    expect(quantJobSchema.safeParse({ ...success, id: null, attempt_count: null }).success).toBe(true)
    for (const invalid of [{ ...failed, artifact_id: A }, { ...failed, error_code: null }, { ...success, analysis_hash: null }, { ...success, id: null }, { ...failed, status: 'pending' }]) expect(quantJobSchema.safeParse(invalid).success).toBe(false)
    expect(reportSchema.safeParse({ ...partialReport(), run: { ...partialReport().run, status: 'completed' } }).success).toBe(false)
  })
  it('正式空池只属于成功策略，失败策略显示尚无初选和当时回执', () => {
    const html = renderToStaticMarkup(createElement(ResearchReportView, { report: partialReport(), onDiscuss: () => {} }))
    expect(html).toContain('量化失败，尚无正式初选')
    expect(html.match(/本策略保留空初选/g)).toHaveLength(1)
    expect(html).toContain('本报告修订的量化状态')
    expect(html).toContain('当前尝试历史独立于上方报告修订')
    expect(html).toContain('量化计算失败')
  })
  it('个股缺产物必须显式不可用，未知分组不渲染为零组', () => {
    const observation = { strategy: 'martin', artifact_id: null, content_hash: null, parameter_hash: 'a'.repeat(64), quant: failed, in_pool: null, calculation_available: false, facts: [], exclusions: [], limitations: [] }
    const stock = stockObservationSchema.parse({ run_id: B, report_id: A, snapshot_id: A, snapshot_hash: 'a'.repeat(64), as_of: '2026-11-20', company: { symbol: 'AAPL', name: 'Apple' }, history: [], history_total: 0, groups: null, limitations: [], observations: [observation, { ...observation, strategy: 'wyckoff', quant: { ...failed, strategy: 'wyckoff' } }] })
    expect(stockObservationSchema.safeParse({ ...stock, observations: [{ ...observation, quant: null }, stock.observations[1]] }).success).toBe(false)
    const html = renderToStaticMarkup(createElement(StockFactsView, { stock }))
    expect(html).toContain('量化尚不可用')
    expect(html).toContain('不能判断组强弱')
    expect(html).not.toContain('未进入量化池')
  })
  it('尝试历史校验分页与运行身份，代理只允许读取', async () => {
    const page = { run_id: B, recorded: true, items: [], total: 0, page: 1, page_size: 10 }
    vi.stubGlobal('fetch', vi.fn(async () => new Response(JSON.stringify({ code: '0000', message: 'ok', correlation_id: A, data: page }))))
    expect((await readQuantAttempts(B, 1)).recorded).toBe(true)
    await expect(readQuantAttempts(A, 1)).rejects.toThrow('量化尝试与当前运行不一致')
    expect(quantAttemptPageSchema.safeParse({ ...page, recorded: false, total: 2 }).success).toBe(false)
    expect(allowedResearchRoute('GET', `/v1/research-runs/${B}/quant-attempts`)).toBe(true)
    expect(allowedResearchRoute('POST', `/v1/research-runs/${B}/quant-attempts`)).toBe(false)
  })
})
