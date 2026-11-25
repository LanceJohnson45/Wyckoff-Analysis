import { renderToStaticMarkup } from 'react-dom/server'
import { createElement } from 'react'
import { describe, expect, it } from 'vitest'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'
import { ExperimentJobView, ResearchParameterExperiments } from './parameter-experiments'
import { businessNumber, experimentJobSchema, experimentResultSchema } from './evaluation-contract'

const ID = '01890a5d-ac96-774b-bcce-b302099a8057'
const RESULT = '01890a5d-ac96-774b-bcce-b302099a8058'
const TIME = '2026-11-24T10:00:00Z'

const job = experimentJobSchema.parse({
  id: ID, kind: 'parameter_experiments', profile: 'baseline-v1', cutoff: '2026-11-20',
  field: 'support_atr', values: [0.5, 0.75, 1.0], status: 'completed', error_code: null,
  result_id: RESULT, cancel_requested: false, attempt_count: 1, create_time: TIME, update_time: TIME,
})

describe('parameter experiments', () => {
  it('keeps three explicit values separate from production parameters', () => {
    expect(experimentJobSchema.safeParse({ ...job, values: [0.5, 0.5, 1.0] }).success).toBe(false)
    expect(experimentResultSchema.safeParse({
      id: RESULT, job_id: ID, kind: 'parameter_experiments', profile: job.profile, cutoff: job.cutoff,
      field: job.field, values: job.values, input_hash: 'a'.repeat(64), source_hash: 'b'.repeat(64),
      variants: [0.5, 0.75, 1].map(value => ({
        report_hash: 'c'.repeat(64), input_hash: 'a'.repeat(64), parameter_hash: 'd'.repeat(64),
        value, candidate_count: 1, usable_sections: { research: 1 }, limitations: ['synthetic'],
      })), manual_review: 'pending', limitations: ['synthetic'],
    }).success).toBe(true)
  })

  it('renders a stable business number and only planned proxy routes', () => {
    expect(businessNumber(TIME, ID)).toMatch(/^SR-\d{8}-US-[0-9A-F]{6}$/)
    expect(renderToStaticMarkup(createElement(ResearchParameterExperiments, { initialCutoff: job.cutoff }))).toContain('参数实验')
    const allowed = [
      ['GET', '/v1/parameter-experiments', true], ['POST', '/v1/parameter-experiments', true],
      ['GET', `/v1/parameter-experiments/${ID}`, true], ['POST', `/v1/parameter-experiments/${ID}/retry`, true],
      ['GET', `/v1/parameter-experiment-results/${RESULT}`, true], ['DELETE', '/v1/parameter-experiments', false],
    ] as const
    for (const [method, path, expected] of allowed) expect(allowedResearchRoute(method, path)).toBe(expected)
  })

  it('显式取消不是冻结输入错误，原输入续跑保持可见', () => {
    const cancelled = experimentJobSchema.parse({ ...job, status: 'cancelled', error_code: 'evaluation_interrupted', result_id: null, cancel_requested: true })
    const actions = { act: async () => undefined, busy: null, pending: null, uncertain: false, readbackId: null, message: '', blocked: false }
    const html = renderToStaticMarkup(createElement(ExperimentJobView, { job: cancelled, actions }))
    expect(html).toContain('已取消；原冻结输入保留')
    expect(html).toContain('按原输入续跑')
    expect(html).not.toContain('任务未完成')
  })
})
