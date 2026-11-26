import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { createEvaluation, readEvaluation, readEvaluations, readEvaluationResult, readEvaluationSamples } from './api'
import { evaluationJobSchema, evaluationSampleSchema, outcomeWindowSchema } from './evaluation-contract'
import { EvaluationJobView } from './evaluation-job'
import { EvaluationSampleRow } from './evaluation-result'
import { ResearchEvaluations } from './evaluations'

const ID = '11111111-1111-4111-8111-111111111111'
const RESULT = '22222222-2222-4222-8222-222222222222'
const job = { id: ID, kind: 'historical_quant_pool', profile: 'synthetic-contract', cutoff: '2026-11-20', allow_current_universe: false, status: 'running', completed_sections: 4, error_code: null, result_id: null, cancel_requested: false, attempt_count: 1, create_time: '2026-11-25T01:00:00+08:00', update_time: '2026-11-25T01:01:00+08:00' }
const window = { horizon: 1, target_session: '2026-11-23', status: 'completed', missing_sessions: [], gross_return_pct: 1, cost_pct: 0.1, net_return_pct: 0.9, benchmark_return_pct: 0.5, net_excess_pct: 0.4, mfe_pct: 2, mae_pct: -1 }
const windows = [1, 3, 5, 10, 20].map(horizon => ({ ...window, horizon }))
const sample = { id: ID, result_id: RESULT, as_of: '2026-11-20', strategy: 'martin', symbol: 'AAPL', partition: 'research', purged: false, status: 'completed', windows }
const envelope = (data: unknown) => ({ code: '0000', message: 'ok', data, correlation_id: ID })

afterEach(() => vi.unstubAllGlobals())

describe('历史评估响应契约', () => {
  it('完成必须有结果和全部截面，取消意图不冒充已取消', () => {
    expect(evaluationJobSchema.parse({ ...job, cancel_requested: true }).status).toBe('running')
    expect(evaluationJobSchema.safeParse({ ...job, status: 'completed' }).success).toBe(false)
    expect(evaluationJobSchema.safeParse({ ...job, status: 'completed', completed_sections: 20, result_id: RESULT }).success).toBe(true)
    expect(evaluationJobSchema.safeParse({ ...job, completed_sections: 21 }).success).toBe(false)
    expect(evaluationJobSchema.safeParse({ ...job, attempt_count: 3 }).success).toBe(false)
  })
  it('封存样本保留身份但不能携带窗口数值', () => {
    expect(evaluationSampleSchema.parse({ ...sample, partition: 'sealed', status: 'sealed', windows: null }).windows).toBeNull()
    expect(evaluationSampleSchema.safeParse({ ...sample, partition: 'sealed', status: 'sealed' }).success).toBe(false)
    expect(evaluationSampleSchema.safeParse({ ...sample, windows: windows.slice(1) }).success).toBe(false)
  })
  it.each(['pending', 'missing_data'])('%s窗口不把未知表现变成零收益', status => {
    const unknown = { ...window, status, gross_return_pct: null, net_return_pct: null, benchmark_return_pct: null, net_excess_pct: null, mfe_pct: null, mae_pct: null }
    expect(outcomeWindowSchema.parse(unknown).net_return_pct).toBeNull()
    expect(outcomeWindowSchema.safeParse({ ...unknown, net_return_pct: 0 }).success).toBe(false)
  })
})

describe('历史评估读取与提交', () => {
  it('网络结果未知时不自动重发，显式核对使用原键和原正文', async () => {
    const fetch = vi.fn().mockRejectedValueOnce(new TypeError('connection lost')).mockResolvedValueOnce(Response.json(envelope(job)))
    vi.stubGlobal('fetch', fetch)
    const body = { profile: job.profile, cutoff: job.cutoff, allow_current_universe: job.allow_current_universe }
    await expect(createEvaluation(body, ID)).rejects.toThrow('评估提交结果未知')
    expect(fetch).toHaveBeenCalledTimes(1)
    expect((await createEvaluation(body, ID)).id).toBe(ID)
    const original = fetch.mock.calls[0]?.[1], confirmation = fetch.mock.calls[1]?.[1]
    expect(confirmation.body).toBe(original.body)
    expect(confirmation.headers['Idempotency-Key']).toBe(ID)
    expect(original.headers['Idempotency-Key']).toBe(ID)
  })
  it('拒绝另一任务、错误页码和错策略样本', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValueOnce(Response.json(envelope({ ...job, id: RESULT }))).mockResolvedValueOnce(Response.json(envelope({ items: [job], total: 1, page: 2, page_size: 5 }))).mockResolvedValueOnce(Response.json(envelope({ items: [sample], total: 1, page: 1, page_size: 20 }))))
    await expect(readEvaluation(ID)).rejects.toThrow('身份不一致')
    await expect(readEvaluations(1)).rejects.toThrow('分页与查询不一致')
    await expect(readEvaluationSamples(RESULT, 1, 'wyckoff')).rejects.toThrow('原结果或查询不一致')
  })
  it('逐股分页走服务端过滤，封存仍保留一条身份记录', async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json(envelope({ items: [{ ...sample, partition: 'sealed', status: 'sealed', windows: null }], total: 23, page: 2, page_size: 20 })))
    vi.stubGlobal('fetch', fetch)
    expect((await readEvaluationSamples(RESULT, 2, 'martin')).items[0]?.status).toBe('sealed')
    expect(fetch.mock.calls[0]?.[0]).toBe(`/api/stock-research/v1/evaluation-results/${RESULT}/samples?page=2&page_size=20&strategy=martin`)
  })
  it('固定结果拒绝回到另一作业', async () => {
    const section = { section: { as_of: '2026-10-01', spy_return_20_pct: 1, environment_rank: 0, partition: 'research', label_end: '2026-10-29', purged: false }, strategy: 'martin', snapshot_hash: 'a'.repeat(64), parameter_hash: 'b'.repeat(64), analysis_hash: 'c'.repeat(64), universe_count: 1, eligible_count: 1, observation_count: 1, exclusions: {}, pool: ['AAPL'] }
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(Response.json(envelope({ id: RESULT, job_id: RESULT, kind: 'historical_quant_pool', report_hash: 'd'.repeat(64), input_hash: 'e'.repeat(64), source_hash: 'f'.repeat(64), parameter_hashes: { martin: 'a'.repeat(64), wyckoff: 'b'.repeat(64) }, cutoff: job.cutoff, allow_current_universe: false, candidate_count: 35, usable_sections: { research: 2, validation: 0, sealed: 4 }, sections: Array.from({ length: 40 }, () => section), model_selection: null, manual_review: 'pending', limitations: ['synthetic contract fixture'] }))))
    await expect(readEvaluationResult(RESULT, ID)).rejects.toThrow('结果不属于原任务')
  })
})

describe('历史评估可见状态', () => {
  it('初始折叠不挂载计算或查询操作', () => {
    const fetch = vi.fn(); vi.stubGlobal('fetch', fetch)
    const html = renderToStaticMarkup(createElement(ResearchEvaluations, { initialCutoff: job.cutoff }))
    expect(html).toContain('历史评估 · 量化池回放')
    expect(html).not.toContain('创建历史评估')
    expect(fetch).not.toHaveBeenCalled()
  })
  it('取消意图保持运行中，等待实际退出且不重复给取消按钮', () => {
    const actions = { act: async () => undefined, busy: null, pending: null, uncertain: false, readbackId: null, message: '', blocked: false }
    const html = renderToStaticMarkup(createElement(EvaluationJobView, { job: evaluationJobSchema.parse({ ...job, profile: '<img src=x>', cancel_requested: true }), actions }))
    expect(html).toContain('运行中')
    expect(html).toContain('等待计算进程实际退出')
    expect(html).not.toContain('取消所选评估')
    expect(html).toContain('&lt;img src=x&gt;')
  })
  it.each(['pending', 'missing_data', 'sealed'])('%s样本明确展示未知状态，保留股票且不显示零收益', status => {
    const unknownWindows = windows.map(item => ({ ...item, status, gross_return_pct: null, net_return_pct: null, benchmark_return_pct: null, net_excess_pct: null, mfe_pct: null, mae_pct: null }))
    const value = evaluationSampleSchema.parse({ ...sample, status, partition: status === 'sealed' ? 'sealed' : 'research', windows: status === 'sealed' ? null : unknownWindows })
    const html = renderToStaticMarkup(createElement('table', null, createElement('tbody', null, createElement(EvaluationSampleRow, { sample: value, horizon: 20 }))))
    expect(html).toContain('AAPL')
    expect(html).toContain(status === 'sealed' ? '表现未开放' : status === 'pending' ? '观察期未满' : '行情缺失')
    expect(html).not.toContain('0.00%')
  })
  it('按所选窗口显示扣费后表现及成本，不误用另一窗口的结果', () => {
    const value = evaluationSampleSchema.parse({ ...sample, purged: true, windows: windows.map(item => item.horizon === 20 ? { ...item, net_return_pct: 3.9, net_excess_pct: 3.4 } : item) })
    const html = renderToStaticMarkup(createElement('table', null, createElement('tbody', null, createElement(EvaluationSampleRow, { sample: value, horizon: 20 }))))
    expect(html).toContain('3.90%')
    expect(html).toContain('3.40%')
    expect(html).toContain('成本 0.10%')
    expect(html).toContain('跨区标签已剔除')
    expect(html).not.toContain('0.90%')
  })
  it.each([
    ['evaluation_history_insufficient', '完整历史截面不足20个'],
    ['evaluation_quality_failed', '历史截面行情质量未通过'],
  ])('%s资料失败说明修复办法，隐藏无效原输入续跑', (code, explanation) => {
    const actions = { act: async () => undefined, busy: null, pending: null, uncertain: false, readbackId: null, message: '', blocked: false }
    const html = renderToStaticMarkup(createElement(EvaluationJobView, { job: evaluationJobSchema.parse({ ...job, status: 'failed', error_code: code }), actions }))
    expect(html).toContain(explanation)
    expect(html).toContain('创建新评估')
    expect(html).not.toContain('按原输入续跑')
  })
  it('显式取消不是调研缺数，已完成截面和原输入续跑分开保留', () => {
    const actions = { act: async () => undefined, busy: null, pending: null, uncertain: false, readbackId: null, message: '', blocked: false }
    const html = renderToStaticMarkup(createElement(EvaluationJobView, { job: evaluationJobSchema.parse({ ...job, status: 'cancelled', error_code: 'evaluation_interrupted', cancel_requested: true }), actions }))
    expect(html).toContain('已取消；已完成截面继续保留')
    expect(html).toContain('按原输入续跑')
    expect(html).not.toContain('任务未完成')
  })
})
