import { z } from 'zod'
import { watchPageSchema, watchEventPageSchema } from './watch-contract'
import { strategySchema, researchId as id, researchDay as day, researchHash as hash } from './contract-values'

export const statusSchema = z.enum(['pending', 'running', 'completed', 'partial', 'failed', 'cancelled'])
const stamp = z.string().datetime({ offset: true })
const point = z.object({ text: z.string(), evidence_refs: z.array(z.string()).min(1) })
const claim = z.object({ ref: z.string(), value: z.union([z.string(), z.number().finite(), z.boolean(), z.null()]) })
const selectedItem = z.object({ symbol: z.string(), rationale: point, risks: z.array(point), confirmation_conditions: z.array(point), claims: z.array(claim) })
const reviewItem = z.object({ symbol: z.string(), stance: z.enum(['support', 'concern', 'insufficient_evidence']), rationale: point, objections: z.array(point), confirmation_conditions: z.array(point), claims: z.array(claim), data_limitations: z.array(z.string()) })
const source = z.enum(['model', 'empty_pool', 'empty_target'])

export const runSchema = z.object({
  run_id: id, market: z.literal('us'), target_session: day, as_of: day,
  snapshot_id: id, snapshot_hash: hash, report_due: stamp, observed_at: stamp, late: z.boolean(), preview: z.boolean(),
  status: statusSchema, error_code: z.string().nullable(), companies: z.array(z.object({ symbol: z.string(), name: z.string().min(1) })),
  quality: z.object({ passed: z.boolean(), coverage: z.number().min(0).max(1), aligned_count: z.number().int(), universe_count: z.number().int(), issues: z.array(z.object({ symbol: z.string(), reason: z.string(), strategy: z.string() })), limitations: z.array(z.string()) }),
  selections: z.array(z.object({ id, strategy: strategySchema, source, decision: z.object({ role: z.literal('select'), strategy: strategySchema, context_hash: hash, items: z.array(selectedItem).max(5), empty_reason: point.nullable() }) })),
  reviews: z.array(z.object({ id, strategy: strategySchema, source, decision: z.object({ role: z.literal('review'), strategy: strategySchema, context_hash: hash, target_selection_id: id, items: z.array(reviewItem).max(5) }) })),
  stages: z.array(z.object({ id, strategy: strategySchema, kind: z.enum(['select', 'review']), status: statusSchema, attempt_count: z.number().int(), target_selection_id: id.nullable(), context_hash: hash.nullable(), error: z.string().nullable(), receipt_available: z.boolean() })),
}).superRefine((run, context) => {
  const names = new Set(run.companies.map((company) => company.symbol))
  for (const selection of run.selections) {
    if (selection.strategy !== selection.decision.strategy || selection.decision.items.some((item) => !names.has(item.symbol))) context.addIssue({ code: z.ZodIssueCode.custom, message: '初选策略或快照公司身份不一致' })
  }
  for (const review of run.reviews) {
    const target = run.selections.find((selection) => selection.id === review.decision.target_selection_id)
    if (!target || target.strategy === review.strategy || review.strategy !== review.decision.strategy || target.decision.items.length !== review.decision.items.length || review.decision.items.some((item) => !target.decision.items.some((selected) => selected.symbol === item.symbol))) context.addIssue({ code: z.ZodIssueCode.custom, message: '互评目标与固定初选不一致' })
  }
})
export const reportSchema = z.object({ report_id: id, revision: z.number().int().positive(), create_time: stamp, content_hash: hash, run: runSchema })
export const runPageSchema = z.object({ items: z.array(z.object({ run_id: id, target_session: day, status: statusSchema, error_code: z.string().nullable(), create_time: stamp })), total: z.number().int(), page: z.number().int(), page_size: z.number().int() })
export const reportPageSchema = z.object({ items: z.array(z.object({ report_id: id, revision: z.number().int(), create_time: stamp, status: statusSchema })), total: z.number().int(), page: z.number().int(), page_size: z.number().int() })
const acceptedSchema = z.object({ run_id: id, status: statusSchema, status_url: z.string(), events_url: z.string() })
const envelope = z.object({ code: z.string(), message: z.string(), data: z.unknown(), correlation_id: id })

export type ResearchRun = z.infer<typeof runSchema>
export type ResearchReport = z.infer<typeof reportSchema>
export type ResearchRunPage = z.infer<typeof runPageSchema>
export type ResearchPoint = z.infer<typeof point>
export type SelectionItem = z.infer<typeof selectedItem>
export type ReviewItem = z.infer<typeof reviewItem>
export type Strategy = z.infer<typeof strategySchema>
export type RunStatus = z.infer<typeof statusSchema>
export interface CreateResearchRun { market: 'us'; target_session: string; preview: true; force?: boolean; parent_run_id?: string }
export const RESEARCH_BASE = '/api/stock-research'

export class ResearchApiError extends Error {
  constructor(message: string, readonly status: number, readonly code: string) { super(message) }
}

export function decodeResearchData<T>(raw: unknown, schema: z.ZodType<T>, status = 200): T {
  const parsed = envelope.safeParse(raw)
  if (!parsed.success) throw new ResearchApiError('研究服务响应格式不正确，请重新连接', status, 'invalid_response')
  if (status >= 400 || parsed.data.code !== '0000') {
    throw new ResearchApiError(parsed.data.message, status, parsed.data.code)
  }
  const result = schema.safeParse(parsed.data.data)
  if (!result.success) throw new ResearchApiError('研究报告字段不完整，请重新连接', status, 'invalid_response')
  return result.data
}

async function request<T>(path: string, schema: z.ZodType<T>, options: RequestInit = {}): Promise<T> {
  const controller = new AbortController()
  const abort = () => controller.abort()
  if (options.signal?.aborted) controller.abort()
  options.signal?.addEventListener('abort', abort, { once: true })
  const timer = setTimeout(abort, options.method === 'POST' ? 130_000 : 20_000)
  try {
    const response = await fetch(RESEARCH_BASE + path, { ...options, signal: controller.signal, cache: 'no-store' })
    let raw: unknown
    try { raw = await response.json() }
    catch { throw new ResearchApiError('研究连接暂不可用，请重新连接', response.status, 'invalid_response') }
    return decodeResearchData(raw, schema, response.status)
  } catch (error) {
    if (error instanceof ResearchApiError) throw error
    throw new ResearchApiError('研究请求未确认，请刷新运行记录后重试', 0, 'connection_unconfirmed')
  } finally {
    clearTimeout(timer)
    options.signal?.removeEventListener('abort', abort)
  }
}

export async function readLatestReport(signal?: AbortSignal, runId?: string): Promise<ResearchReport | null> {
  try { return await request(`/v1/reports/latest${runId ? `?run_id=${id.parse(runId)}` : ''}`, reportSchema, { signal }) }
  catch (error) {
    if (error instanceof ResearchApiError && error.status === 404 && error.code === 'research.not_found') return null
    throw error
  }
}
export function readReport(reportId: string, signal?: AbortSignal): Promise<ResearchReport> {
  return request(`/v1/reports/${id.parse(reportId)}`, reportSchema, { signal })
}
export function readRuns(page: number, signal?: AbortSignal): Promise<ResearchRunPage> {
  return request(`/v1/research-runs?page=${page}&page_size=10`, runPageSchema, { signal })
}
export function readRevisions(runId: string, signal?: AbortSignal) {
  return request(`/v1/research-runs/${id.parse(runId)}/reports?page_size=100`, reportPageSchema, { signal })
}
export function createResearchRun(body: CreateResearchRun, key: string) {
  return request('/v1/research-runs', acceptedSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) })
}
export function controlResearchRun(runId: string, action: 'retry' | 'cancel') {
  return request(`/v1/research-runs/${id.parse(runId)}/${action}`, acceptedSchema, { method: 'POST' })
}

export function readResearchWatchlist(page: number, strategy?: Strategy, signal?: AbortSignal) {
  return request(`/v1/watchlists?page=${page}&page_size=10${strategy ? `&strategy=${strategySchema.parse(strategy)}` : ''}`, watchPageSchema, { signal })
}
export function readResearchWatchEvents(page: number, strategy?: Strategy, signal?: AbortSignal) {
  return request(`/v1/watchlists/events?page=${page}&page_size=10${strategy ? `&strategy=${strategySchema.parse(strategy)}` : ''}`, watchEventPageSchema, { signal })
}
