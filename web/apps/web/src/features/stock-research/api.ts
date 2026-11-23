import { z } from 'zod'
import { watchPageSchema, watchEventPageSchema } from './watch-contract'
import { strategySchema, researchId as id, researchDay as day, researchHash as hash, researchQuality } from './contract-values'
import { stockObservationSchema } from './stock-contract'
import { parameterPageSchema, parameterSetSchema, strategyParameterSchema, strategyParametersSchema, type ParameterValues } from './parameter-contract'
import { preparationPageSchema } from './preparation-contract'
import { chatPageSchema, chatTurnSchema, type CreateChatTurn } from './chat-contract'

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
  quality: researchQuality,
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

async function request<T>(path: string, schema: z.ZodType<T>, options: RequestInit = {}, unconfirmed = '研究请求未确认，请刷新运行记录后重试'): Promise<T> {
  const controller = new AbortController()
  const abort = () => controller.abort()
  if (options.signal?.aborted) controller.abort()
  options.signal?.addEventListener('abort', abort, { once: true })
  const timer = setTimeout(abort, options.method === 'POST' && path.startsWith('/v1/research-runs') ? 130_000 : 20_000)
  try {
    const response = await fetch(RESEARCH_BASE + path, { ...options, signal: controller.signal, cache: 'no-store' })
    let raw: unknown
    try { raw = await response.json() }
    catch { throw new ResearchApiError('研究连接暂不可用，请重新连接', response.status, 'invalid_response') }
    return decodeResearchData(raw, schema, response.status)
  } catch (error) {
    if (error instanceof ResearchApiError) throw error
    throw new ResearchApiError(unconfirmed, 0, 'connection_unconfirmed')
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

export async function readResearchStock(report: ResearchReport, symbol: string, signal?: AbortSignal) {
  if (!/^[A-Z][A-Z0-9.-]{0,31}$/.test(symbol)) throw new ResearchApiError('请输入大写美股代码', 422, 'research.invalid_input')
  const stock = await request(`/v1/stocks/${encodeURIComponent(symbol)}/observations?run_id=${report.run.run_id}&report_id=${report.report_id}&limit=320`, stockObservationSchema, { signal })
  if (stock.run_id !== report.run.run_id || stock.report_id !== report.report_id || stock.snapshot_hash !== report.run.snapshot_hash || stock.snapshot_id !== report.run.snapshot_id || stock.as_of !== report.run.as_of || stock.company.symbol !== symbol) throw new ResearchApiError('个股响应与当前固定报告不一致，请重新连接', 503, 'invalid_response')
  return stock
}

export function readCurrentParameters(signal?: AbortSignal) {
  return request('/v1/strategies', strategyParametersSchema, { signal })
}
export function readFrozenParameters(runId: string, signal?: AbortSignal) {
  return request(`/v1/research-runs/${id.parse(runId)}/parameters`, strategyParametersSchema, { signal })
}
export function readParameterVersions(strategy: Strategy, page: number, signal?: AbortSignal) {
  return request(`/v1/parameter-sets?strategy=${strategySchema.parse(strategy)}&page=${page}&page_size=10`, parameterPageSchema, { signal })
}
export async function registerParameterVersion(strategy: Strategy, parameters: ParameterValues) {
  const saved = await request('/v1/parameter-sets', parameterSetSchema, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ strategy, parameters }) }, '参数登记结果未确认，请刷新版本列表后重试')
  if (saved.strategy !== strategy || saved.parameters.version !== parameters.version) throw new ResearchApiError('参数登记响应不一致，请读取版本列表核对', 503, 'invalid_response')
  return saved
}
export async function publishParameterVersion(strategy: Strategy, parameterId: string, revision: number) {
  const saved = await request(`/v1/strategies/${strategySchema.parse(strategy)}/activate-parameters`, strategyParameterSchema, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ parameter_set_id: id.parse(parameterId), expected_revision: revision }) }, '参数发布结果未确认，请刷新当前参数后再操作')
  if (saved.strategy !== strategy || saved.parameter_set_id !== parameterId || saved.revision !== revision + 1 || saved.source !== 'published') throw new ResearchApiError('参数发布响应不一致，请读取当前配置核对', 503, 'invalid_response')
  return saved
}

export async function readPreparations(target: string, page: number, signal?: AbortSignal) {
  const result = await request(`/v1/preparations?target_session=${day.parse(target)}&page=${page}&page_size=10`, preparationPageSchema, { signal })
  if (result.items.some(item => item.target_session !== target)) throw new ResearchApiError('准备回执日期与当前查询不一致，请重新连接', 503, 'invalid_response')
  return result
}

export async function readChatTurns(reportId: string, page: number, conversationId?: string, signal?: AbortSignal) {
  const result = await request(`/v1/chat-turns?report_id=${id.parse(reportId)}&page=${page}&page_size=10${conversationId ? `&conversation_id=${id.parse(conversationId)}` : ''}`, chatPageSchema, { signal })
  if (result.items.some(turn => turn.report_id !== reportId || (conversationId && turn.conversation_id !== conversationId))) throw new ResearchApiError('问答历史与当前报告或会话不一致', 503, 'invalid_response')
  return result
}
export async function readChatTurn(turnId: string, reportId: string, signal?: AbortSignal) {
  const result = await request(`/v1/chat-turns/${id.parse(turnId)}`, chatTurnSchema, { signal })
  if (result.id !== turnId || result.report_id !== reportId) throw new ResearchApiError('问答响应与当前报告不一致', 503, 'invalid_response')
  return result
}
export async function createChatTurn(body: CreateChatTurn, key: string) {
  const result = await request('/v1/chat-turns', chatTurnSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '问答提交结果未确认，请确认原提交后再发送新问题')
  if (result.report_id !== body.report_id || result.question !== body.question.trim() || result.parent_turn_id !== (body.parent_turn_id || null)) throw new ResearchApiError('问答提交响应不一致，请确认原提交', 503, 'invalid_response')
  return result
}
export async function cancelChatTurn(turnId: string, reportId: string) {
  const result = await request(`/v1/chat-turns/${id.parse(turnId)}/cancel`, chatTurnSchema, { method: 'POST' }, '取消结果未确认，请刷新问答状态')
  if (result.id !== turnId || result.report_id !== reportId) throw new ResearchApiError('取消响应与当前问答不一致', 503, 'invalid_response')
  return result
}
