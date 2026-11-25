import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  createSelectionObservation,
  readSelectionObservation,
  readSelectionObservationResult,
  readSelectionObservationSamples,
  readSelectionObservations,
} from './api'
import {
  type SelectionObservationResult,
  outcomeWindowSchema,
  selectionObservationJobSchema,
  selectionObservationResultSchema,
} from './evaluation-contract'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'
import { ObservationJobView, ResearchSelectionObservations } from './selection-observations'

const ID = '11111111-1111-4111-8111-111111111111'
const REPORT = '22222222-2222-4222-8222-222222222222'
const RESULT = '33333333-3333-4333-8333-333333333333'
const job = {
  id: ID, kind: 'formal_selection_observations', report_id: REPORT, cutoff: '2026-11-20',
  status: 'running', error_code: null, result_id: null, cancel_requested: false, attempt_count: 1,
  create_time: '2026-11-25T01:00:00+08:00', update_time: '2026-11-25T01:01:00+08:00',
}
const envelope = (data: unknown) => ({ code: '0000', message: 'ok', data, correlation_id: ID })

afterEach(() => vi.unstubAllGlobals())

describe('正式初选观测读取与提交', () => {
  it('网络结果未知时不自动重发，显式核对使用原键和原正文', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('connection lost')).mockResolvedValueOnce(Response.json(envelope(job)))
    vi.stubGlobal('fetch', fetch)
    const body = { report_id: REPORT, cutoff: job.cutoff }
    await expect(createSelectionObservation(body, ID)).rejects.toThrow('正式观测提交结果未知')
    expect(fetch).toHaveBeenCalledTimes(1)
    expect((await createSelectionObservation(body, ID)).id).toBe(ID)
    const original = fetch.mock.calls[0]?.[1], confirmation = fetch.mock.calls[1]?.[1]
    expect(confirmation.body).toBe(original.body)
    expect(confirmation.headers['Idempotency-Key']).toBe(ID)
    expect(original.headers['Idempotency-Key']).toBe(ID)
  })

  it('拒绝另一任务、错误页码和错结果样本', async () => {
    vi.stubGlobal('fetch', vi.fn()
      .mockResolvedValueOnce(Response.json(envelope({ ...job, id: RESULT })))
      .mockResolvedValueOnce(Response.json(envelope({ items: [job], total: 1, page: 2, page_size: 5 })))
      .mockResolvedValueOnce(Response.json(envelope({ items: [], total: 1, page: 1, page_size: 20 }))))
    await expect(readSelectionObservation(ID)).rejects.toThrow('正式观测任务身份不一致')
    await expect(readSelectionObservations(1)).rejects.toThrow('正式观测分页与查询不一致')
    await expect(readSelectionObservationSamples(RESULT, 1)).rejects.toThrow('正式逐股观测与原结果或查询不一致')
  })

  it('固定结果拒绝回到另一作业', async () => {
    const result = fixtureResult()
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(Response.json(envelope(result))))
    expect((await readSelectionObservationResult(RESULT, ID)).id).toBe(RESULT)
    await expect(readSelectionObservationResult(RESULT, RESULT)).rejects.toThrow('正式观测结果不属于原任务')
  })
})

describe('正式初选观测可见状态', () => {
  it('初始折叠不挂载查询操作', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch)
    const html = renderToStaticMarkup(createElement(ResearchSelectionObservations, { reportId: REPORT, initialCutoff: job.cutoff }))
    expect(html).toContain('正式初选观测 · 提交后开盘表现')
    expect(html).not.toContain('创建正式观测')
    expect(fetch).not.toHaveBeenCalled()
  })

  it('空名单是有效观察证据，不显示为零表现', () => {
    const result = selectionObservationResultSchema.parse({ ...fixtureResult(), selection_count: 0, observation_count: 0 })
    const html = renderToStaticMarkup(createElement(StaticObservationResult, { result }))
    expect(html).toContain('入选 0 份 · 逐股观察 0 条')
    expect(html).toContain('空名单仍是有效正式观察')
    expect(html).not.toContain('0.00%')
  })

  it.each(['pending', 'missing_data'])('%s窗口不把未知表现变成零收益', status => {
    const value = outcomeWindowSchema.parse({
      horizon: 1, target_session: '2026-11-23', status, missing_sessions: [],
      gross_return_pct: null, cost_pct: 0, net_return_pct: null, benchmark_return_pct: null,
      net_excess_pct: null, mfe_pct: null, mae_pct: null,
    })
    expect(value.net_return_pct).toBeNull()
    expect(outcomeWindowSchema.safeParse({ ...value, net_return_pct: 0 }).success).toBe(false)
  })

  it('显式取消不是数据调研失败，原任务仍按原输入续跑', () => {
    const cancelled = selectionObservationJobSchema.parse({ ...job, status: 'cancelled', error_code: 'evaluation_interrupted', cancel_requested: true })
    const actions = { act: async () => undefined, busy: null, pending: null, uncertain: false, readbackId: null, message: '', blocked: false }
    const html = renderToStaticMarkup(createElement(ObservationJobView, { job: cancelled, actions }))
    expect(html).toContain('已取消；原任务可按原报告和截止日续跑')
    expect(html).toContain('按原输入续跑')
    expect(html).not.toContain('任务未完成')
  })

  it.each<[string, string, boolean]>([
    ['GET', '/v1/selection-observations', true],
    ['POST', '/v1/selection-observations', true],
    ['GET', `/v1/selection-observations/${ID}`, true],
    ['POST', `/v1/selection-observations/${ID}/retry`, true],
    ['POST', `/v1/selection-observations/${ID}/cancel`, true],
    ['GET', `/v1/selection-observation-results/${RESULT}`, true],
    ['GET', `/v1/selection-observation-results/${RESULT}/samples`, true],
    ['POST', `/v1/selection-observation-results/${RESULT}/samples`, false],
    ['GET', `/v1/selection-observations/${ID}/samples`, false],
    ['POST', `/v1/selection-observations/${ID}/arbitrary`, false],
  ])('正式观测代理 %s %s 的显式边界为 %s', (method, path, allowed) => {
    expect(allowedResearchRoute(method, path)).toBe(allowed)
  })
})

function fixtureResult(): SelectionObservationResult {
  return {
    id: RESULT, job_id: ID, kind: 'formal_selection_observations',
    algorithm: 'selection-forward-open-v1', reference: 'first_open_after_commit',
    scope: {
      workspace: 'synthetic-contract', report_id: REPORT, report_hash: 'a'.repeat(64), report_revision: 1,
      report_status: 'completed', run_id: ID, snapshot_id: ID, snapshot_hash: 'b'.repeat(64),
      target_session: '2026-11-23', selections: [],
    },
    cutoff: job.cutoff, report_hash: 'a'.repeat(64), input_hash: 'c'.repeat(64), source_hash: 'd'.repeat(64),
    source_identity: 'synthetic source', costs: {}, selection_count: 0, observation_count: 0,
    missing_strategies: [], manual_review: 'pending', limitations: ['空名单仍是有效正式观察'],
  }
}

function StaticObservationResult({ result }: { result: SelectionObservationResult }) {
  return <section aria-label="固定正式初选观测结果">
    <p>入选 {result.selection_count} 份 · 逐股观察 {result.observation_count} 条</p>
    {result.limitations.map(text => <p key={text}>{text}</p>)}
  </section>
}
