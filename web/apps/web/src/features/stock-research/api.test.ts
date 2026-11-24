import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it, vi } from 'vitest'
import { decodeResearchData, readLatestReport, reportSchema, type ResearchReport } from './api'
import { newestReport, newYorkDay } from './state'
import { ResearchReportView } from './report'
import { allowedResearchRoute, localResearchEnabled, stockResearchProxy, trustedResearchRequest } from '../../../server/stock-research-proxy'
import type { ViteDevServer } from 'vite'

const RUN = '11111111-1111-4111-8111-111111111111'
const SELECTION = '22222222-2222-4222-8222-222222222222'
const REPORT = '33333333-3333-4333-8333-333333333333'
const SNAPSHOT = '44444444-4444-4444-8444-444444444444'
const REVIEW = '55555555-5555-4555-8555-555555555555'
const point = { text: '合成接口样本，仅检验显示与引用', evidence_refs: ['martin/AAPL/factors/close'] }

function fixtureReport(): ResearchReport {
  return reportSchema.parse({
    report_id: REPORT, revision: 3, create_time: '2026-11-23T22:00:00+08:00', content_hash: 'a'.repeat(64),
    run: { run_id: RUN, market: 'us', target_session: '2026-11-23', as_of: '2026-11-20', snapshot_id: SNAPSHOT, snapshot_hash: 'b'.repeat(64), report_due: '2026-11-23T21:30:00+08:00', observed_at: '2026-11-23T22:00:00+08:00', late: true, preview: true, status: 'completed', error_code: null,
      companies: [{ symbol: 'AAPL', name: 'Apple' }], quality: { passed: true, coverage: 1, aligned_count: 1, universe_count: 1, issues: [], limitations: ['synthetic interface fixture'] },
      selections: [{ id: SELECTION, strategy: 'martin', source: 'model', decision: { role: 'select', strategy: 'martin', context_hash: 'c'.repeat(64), empty_reason: null, items: [{ symbol: 'AAPL', rationale: point, risks: [point], confirmation_conditions: [point], claims: [{ ref: 'martin/AAPL/factors/close', value: 11 }] }] } }],
      reviews: [{ id: REVIEW, strategy: 'wyckoff', source: 'model', decision: { role: 'review', strategy: 'wyckoff', context_hash: 'd'.repeat(64), target_selection_id: SELECTION, items: [{ symbol: 'AAPL', stance: 'concern', rationale: point, objections: [point], confirmation_conditions: [], claims: [{ ref: 'bar/stock/AAPL/2026-11-20/close', value: 11 }], data_limitations: [] }] } }], stages: [],
    },
  })
}

const envelope = (data: unknown) => ({ code: '0000', message: 'ok', data, correlation_id: RUN })

describe('研究报告契约', () => {
  it('远端快照读取超过20秒仍可完成，但60秒无响应会明确失败', async () => {
    vi.useFakeTimers()
    try {
      vi.stubGlobal('fetch', vi.fn((_url: string, options: RequestInit) => new Promise<Response>((resolve, reject) => {
        const timer = setTimeout(() => resolve(Response.json(envelope(fixtureReport()))), 30_000)
        options.signal?.addEventListener('abort', () => { clearTimeout(timer); reject(new Error('aborted')) }, { once: true })
      })))
      const delayed = readLatestReport()
      await vi.advanceTimersByTimeAsync(30_000)
      expect((await delayed)?.report_id).toBe(REPORT)
      vi.stubGlobal('fetch', vi.fn((_url: string, options: RequestInit) => new Promise<Response>((_resolve, reject) => {
        options.signal?.addEventListener('abort', () => reject(new Error('aborted')), { once: true })
      })))
      const expired = expect(readLatestReport()).rejects.toThrow('研究请求未确认')
      await vi.advanceTimersByTimeAsync(60_000)
      await expired
    } finally {
      vi.unstubAllGlobals()
      vi.useRealTimers()
    }
  })
  it('解包固定报告并拒绝把失败或缺字段当作空初选', () => {
    expect(decodeResearchData(envelope(fixtureReport()), reportSchema).run.selections).toHaveLength(1)
    expect(() => decodeResearchData({ ...envelope(null), code: 'research.unavailable', message: '来源未就绪' }, reportSchema, 503)).toThrow('来源未就绪')
    expect(() => decodeResearchData(envelope({}), reportSchema)).toThrow('字段不完整')
    expect(() => decodeResearchData({ code: 0, data: fixtureReport() }, reportSchema)).toThrow('格式不正确')
  })
  it('拒绝缺公司身份及引用另一初选的互评', () => {
    const valid = fixtureReport()
    expect(() => reportSchema.parse({ ...valid, run: { ...valid.run, companies: [] } })).toThrow()
    const reviews = valid.run.reviews.map((review) => ({ ...review, decision: { ...review.decision, target_selection_id: RUN } }))
    expect(() => reportSchema.parse({ ...valid, run: { ...valid.run, reviews } })).toThrow()
  })
  it('较晚到达的旧查询不能覆盖SSE新修订', () => {
    const older = fixtureReport(), newer = { ...older, revision: 4 }
    expect(newestReport(newer, older)).toBe(newer)
    expect(newestReport(older, newer)).toBe(newer)
  })
  it('分别显示理由和质疑，转义外部观点文本', () => {
    const report = fixtureReport()
    const item = report.run.selections[0]?.decision.items[0]
    if (!item) throw new Error('fixture missing item')
    item.rationale.text = '<img src=x onerror=alert(1)>'
    const html = renderToStaticMarkup(createElement(ResearchReportView, { report, onDiscuss: () => undefined }))
    expect(html).toContain('Apple')
    expect(html).toContain('AAPL')
    expect(html).toContain('Wyckoff 量价结构 · 质疑')
    expect(html).toContain('迟到报告')
    expect(html).toContain('实际行情 2026-11-20')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
  })
  it('纽约日期在中国跨日后保持真实美国日期', () => {
    expect(newYorkDay(new Date('2026-11-23T02:00:00Z'))).toBe('2026-11-22')
    expect(newYorkDay(new Date('2026-07-04T02:00:00Z'))).toBe('2026-07-03')
  })
})

describe('本机研究代理策略', () => {
  it('单用户须显式启用代理，生产构建始终保留账号流程', () => {
    const environment = { STOCK_RESEARCH_WEB_SINGLE_USER: 'true', STOCK_RESEARCH_WEB_PROXY_ENABLED: 'true' }
    expect(localResearchEnabled({}, 'serve')).toBe(false)
    expect(localResearchEnabled(environment, 'serve')).toBe(true)
    expect(localResearchEnabled(environment, 'build')).toBe(false)
    expect(() => localResearchEnabled({ STOCK_RESEARCH_WEB_SINGLE_USER: 'true' }, 'serve')).toThrow('研究代理')
  })
  it('单用户代理拒绝公网绑定和缺失服务端身份', () => {
    const environment = { STOCK_RESEARCH_WEB_PROXY_ENABLED: 'true', STOCK_RESEARCH_API_TOKEN: 't'.repeat(32) }
    const start = (values: Record<string, string>, host: string | boolean) => {
      const plugin = stockResearchProxy(values)
      if (typeof plugin.configureServer !== 'function') throw new Error('missing server hook')
      return plugin.configureServer({ config: { server: { host } } } as ViteDevServer)
    }
    for (const host of ['0.0.0.0', '::', true, '192.168.1.2']) expect(() => start(environment, host)).toThrow('本机监听')
    expect(() => start({ ...environment, STOCK_RESEARCH_API_TOKEN: '' }, '127.0.0.1')).toThrow('独立身份')
  })
  it('只接收本机同源请求，拒绝跨站和伪造Host', () => {
    expect(trustedResearchRequest({ host: '127.0.0.1:5173', origin: 'http://127.0.0.1:5173', 'sec-fetch-site': 'same-origin' })).toBe(true)
    expect(trustedResearchRequest({ host: '127.0.0.1:5173', origin: 'https://untrusted.invalid' })).toBe(false)
    expect(trustedResearchRequest({ host: 'untrusted.invalid' })).toBe(false)
    expect(trustedResearchRequest({ host: 'localhost:5173', 'sec-fetch-site': 'cross-site' })).toBe(false)
  })
  it('固定路由与方法，不提供任意地址或文件读取', () => {
    expect(allowedResearchRoute('GET', '/v1/reports/latest')).toBe(true)
    for (const resource of ['market', 'groups', 'compare', 'rules']) {
      expect(allowedResearchRoute('GET', `/v1/reports/${REPORT}/${resource}`)).toBe(true)
      expect(allowedResearchRoute('POST', `/v1/reports/${REPORT}/${resource}`)).toBe(false)
    }
    expect(allowedResearchRoute('GET', '/v1/reports/latest/market')).toBe(false)
    expect(allowedResearchRoute('GET', `/v1/research-runs/${RUN}/events`)).toBe(true)
    expect(allowedResearchRoute('GET', '/v1/watchlists/events')).toBe(true)
    expect(allowedResearchRoute('GET', '/v1/stocks/ACGL/observations')).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/watchlists')).toBe(false)
    expect(allowedResearchRoute('POST', `/v1/research-runs/${RUN}/retry`)).toBe(true)
    expect(allowedResearchRoute('DELETE', `/v1/research-runs/${RUN}`)).toBe(false)
    expect(allowedResearchRoute('POST', '/v1/reports/latest')).toBe(false)
    expect(allowedResearchRoute('GET', '/../../.env')).toBe(false)
    expect(allowedResearchRoute('GET', '//untrusted.invalid/v1/reports/latest')).toBe(false)
  })
  it.each<[string, string, boolean]>([
    ['GET', '/v1/evaluations', true],
    ['POST', '/v1/evaluations', true],
    ['GET', `/v1/evaluations/${RUN}`, true],
    ['POST', `/v1/evaluations/${RUN}/retry`, true],
    ['POST', `/v1/evaluations/${RUN}/cancel`, true],
    ['GET', `/v1/evaluation-results/${RUN}`, true],
    ['GET', `/v1/evaluation-results/${RUN}/samples`, true],
    ['POST', `/v1/evaluation-results/${RUN}/samples`, false],
    ['GET', `/v1/evaluations/${RUN}/samples`, false],
    ['GET', `/v1/evaluation-results/${RUN}/input.json`, false],
    ['POST', `/v1/evaluations/${RUN}/arbitrary`, false],
  ])('评估代理 %s %s 的显式边界为 %s', (method, path, allowed) => {
    expect(allowedResearchRoute(method, path)).toBe(allowed)
  })
})
