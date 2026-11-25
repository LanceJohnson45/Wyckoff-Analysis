import { z } from 'zod'
import { quantJobSchema, quantAttemptPageSchema } from './quant-contract'
import { anchorChartSchema } from './anchor-contract'
import { watchPageSchema, watchEventPageSchema } from './watch-contract'
import { strategySchema, researchId as id, researchDay as day, researchHash as hash, researchQuality } from './contract-values'
import { stockObservationSchema } from './stock-contract'
import { parameterPageSchema, parameterSetSchema, strategyParameterSchema, strategyParametersSchema, type ParameterValues } from './parameter-contract'
import { preparationPageSchema } from './preparation-contract'
import { chatPageSchema, chatTurnSchema, type CreateChatTurn } from './chat-contract'
import { universePageSchema } from './universe-contract'
import {
  evaluationJobPageSchema,
  evaluationJobSchema,
  evaluationResultSchema,
  evaluationSamplePageSchema,
  experimentJobPageSchema,
  experimentJobSchema,
  experimentResultSchema,
  selectionObservationJobPageSchema,
  selectionObservationJobSchema,
  selectionObservationResultSchema,
  selectionObservationSamplePageSchema,
  type CreateEvaluation,
  type CreateSelectionObservation,
  type CreateExperiment,
} from './evaluation-contract'
import { renewResearchWebSession } from './web-session'
import { operationsSchema } from './operations-contract'
import { themeProposalPageSchema, themeProposalSchema, inputPublicationPageSchema, inputPublicationSchema, manualHeadSchema, frozenInputsSchema, canonicalResearchSymbol, type ThemeDefinition, type ConfirmTheme, type CreateManualOverride } from './input-contract'

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
  quality: researchQuality, quant: z.array(quantJobSchema).default([]),
  selections: z.array(z.object({ id, strategy: strategySchema, source, decision: z.object({ role: z.literal('select'), strategy: strategySchema, context_hash: hash, items: z.array(selectedItem).max(5), empty_reason: point.nullable() }) })),
  reviews: z.array(z.object({ id, strategy: strategySchema, source, decision: z.object({ role: z.literal('review'), strategy: strategySchema, context_hash: hash, target_selection_id: id, items: z.array(reviewItem).max(5) }) })),
  stages: z.array(z.object({ id, strategy: strategySchema, kind: z.enum(['select', 'review']), status: statusSchema, attempt_count: z.number().int(), target_selection_id: id.nullable(), context_hash: hash.nullable(), error: z.string().nullable(), receipt_available: z.boolean() })),
}).superRefine((run, context) => {
  if (run.quant.length > 0 && (run.quant.length !== 2 || new Set(run.quant.map(row => row.strategy)).size !== 2 || (run.status === 'completed' && run.quant.some(row => row.status !== 'completed')))) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化回执策略缺失或与完成状态不一致' })
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

export function submissionUnconfirmed(error: unknown, reconciling = false): boolean {
  // 拒绝本次核对不能证明较早的提交未受理。
  return reconciling || error instanceof ResearchApiError && (error.status === 0 || error.status >= 500 || error.code === 'invalid_response')
}

export function decodeResearchData<T>(raw: unknown, schema: z.ZodType<T, z.ZodTypeDef, unknown>, status = 200): T {
  const parsed = envelope.safeParse(raw)
  if (!parsed.success) throw new ResearchApiError('研究服务响应格式不正确，请重新连接', status, 'invalid_response')
  if (status >= 400 || parsed.data.code !== '0000') {
    throw new ResearchApiError(parsed.data.message, status, parsed.data.code)
  }
  const result = schema.safeParse(parsed.data.data)
  if (!result.success) throw new ResearchApiError('研究报告字段不完整，请重新连接', status, 'invalid_response')
  return result.data
}

async function request<T>(path: string, schema: z.ZodType<T, z.ZodTypeDef, unknown>, options: RequestInit = {}, unconfirmed = '研究请求未确认，请刷新运行记录后重试'): Promise<T> {
  const controller = new AbortController()
  const abort = () => controller.abort()
  if (options.signal?.aborted) controller.abort()
  options.signal?.addEventListener('abort', abort, { once: true })
  // 冻结快照读取与其他页面查询共用远端PG连接，须给排队和传输留出有界时间。
  const timeout = !options.method || options.method === 'GET' ? 60_000 : options.method === 'POST' && path.startsWith('/v1/research-runs') ? 130_000 : 20_000
  const timer = setTimeout(abort, timeout)
  try {
    const fetchOptions: RequestInit = { ...options, signal: controller.signal, cache: 'no-store' }
    let response = await fetch(RESEARCH_BASE + path, fetchOptions)
    if (response.status === 401 && response.headers.get('X-Research-Web-Session') === 'required' && await renewResearchWebSession() && !controller.signal.aborted) response = await fetch(RESEARCH_BASE + path, fetchOptions)
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

export async function readResearchUniverse(report: ResearchReport, page: number, search: string, signal?: AbortSignal) {
  const result = await request(`/v1/reports/${id.parse(report.report_id)}/universe?page=${page}&page_size=20${search ? `&search=${encodeURIComponent(search)}` : ''}`, universePageSchema, { signal })
  if (result.report_id !== report.report_id || result.run_id !== report.run.run_id || result.snapshot_id !== report.run.snapshot_id || result.snapshot_hash !== report.run.snapshot_hash || result.as_of !== report.run.as_of || result.target_session !== report.run.target_session || result.page !== page || result.page_size !== 20 || result.search !== (search || null)) throw new ResearchApiError('股票池响应与当前固定报告或查询不一致', 503, 'invalid_response')
  return result
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

export function readThemeProposals(page: number, signal?: AbortSignal) {
  return request(`/v1/theme-proposals?page=${page}&page_size=5`, themeProposalPageSchema, { signal })
}
export async function readThemeProposal(identifier: string) {
  const result = await request(`/v1/theme-proposals/${id.parse(identifier)}`, themeProposalSchema)
  if (result.id !== identifier) throw new ResearchApiError('提议响应与当前对象不一致', 503, 'invalid_response')
  return result
}
export async function proposeTheme(body: ThemeDefinition, key: string) {
  const result = await request('/v1/theme-proposals', themeProposalSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '提议提交未确认，请使用原提交核对')
  if (result.definition.name !== body.name.trim() || result.definition.members.join(',') !== [...body.members].sort().join(',')) throw new ResearchApiError('提议响应与原成员不一致，请核对原提交', 503, 'invalid_response')
  return result
}
export async function confirmTheme(identifier: string, body: ConfirmTheme) {
  const result = await request(`/v1/theme-proposals/${id.parse(identifier)}/confirm`, inputPublicationSchema, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }, '主题确认结果未知，请核对原确认')
  if (result.proposal_id !== identifier || result.revision !== body.expected_revision + 1 || result.effective_date !== body.effective_date) throw new ResearchApiError('主题确认响应不一致，请核对原确认', 503, 'invalid_response')
  return result
}
export function readManualHistory(page: number, signal?: AbortSignal) { return request(`/v1/universe-overrides?page=${page}&page_size=5`, inputPublicationPageSchema, { signal }) }
export async function readManualHead(symbol: string) {
  const requested = canonicalResearchSymbol(symbol)
  const result = await request(`/v1/universe-overrides/${encodeURIComponent(requested)}`, manualHeadSchema)
  if (result.symbol !== requested) throw new ResearchApiError('自选响应与当前代码不一致', 503, 'invalid_response')
  return result
}
export async function publishManual(body: CreateManualOverride, key: string) {
  const result = await request('/v1/universe-overrides', inputPublicationSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '自选发布结果未知，请核对原提交')
  if (result.kind !== 'manual' || result.target !== body.symbol || result.revision !== body.expected_revision + 1 || result.effective_date !== body.effective_date) throw new ResearchApiError('自选发布响应不一致，请核对原提交', 503, 'invalid_response')
  return result
}
export async function readFrozenInputs(runId: string, signal?: AbortSignal) {
  const result = await request(`/v1/research-runs/${id.parse(runId)}/inputs`, frozenInputsSchema, { signal })
  if (result.run_id !== runId) throw new ResearchApiError('输入来源不属于当前运行', 503, 'invalid_response')
  return result
}

export async function readResearchAnchorChart(report: ResearchReport, symbol: string, index: number | null, signal?: AbortSignal) {
  if (!/^[A-Z][A-Z0-9.-]{0,31}$/.test(symbol) || (index !== null && (!Number.isInteger(index) || index < 0))) throw new ResearchApiError('锚点查询参数无效', 422, 'invalid_input')
  const value = await request(`/v1/reports/${id.parse(report.report_id)}/anchors/${symbol}?limit=320${index === null ? '' : `&index=${index}`}`, anchorChartSchema, { signal })
  if (value.report_id !== report.report_id || value.run_id !== report.run.run_id || value.snapshot_id !== report.run.snapshot_id || value.snapshot_hash !== report.run.snapshot_hash || value.as_of !== report.run.as_of || value.company.symbol !== symbol || (index !== null && value.selected_index !== index)) throw new ResearchApiError('锚点与当前冻结报告不一致', 502, 'invalid_response')
  return value
}

export async function readQuantAttempts(runId: string, page: number, signal?: AbortSignal) {
  const value = await request(`/v1/research-runs/${id.parse(runId)}/quant-attempts?page=${page}&page_size=10`, quantAttemptPageSchema, { signal })
  if (value.run_id !== runId || value.page !== page || value.page_size !== 10) throw new ResearchApiError('量化尝试与当前运行不一致', 502, 'invalid_response')
  return value
}

export async function readEvaluations(page: number, signal?: AbortSignal) {
  const value = await request(`/v1/evaluations?page=${page}&page_size=5`, evaluationJobPageSchema, { signal })
  if (value.page !== page || value.page_size !== 5) throw new ResearchApiError('评估分页与查询不一致', 503, 'invalid_response')
  return value
}
export async function readOperations(target: string, signal?: AbortSignal) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(target)) throw new ResearchApiError('目标日期无效', 422, 'invalid_input')
  const value = await request(`/v1/operations?target_session=${target}`, operationsSchema, { signal })
  if (value.target_session !== target) throw new ResearchApiError('操作状态与目标日不一致', 503, 'invalid_response')
  return value
}
export async function readEvaluation(identifier: string, signal?: AbortSignal) {
  const value = await request(`/v1/evaluations/${id.parse(identifier)}`, evaluationJobSchema, { signal })
  if (value.id !== identifier) throw new ResearchApiError('评估任务身份不一致', 503, 'invalid_response')
  return value
}
export async function createEvaluation(body: CreateEvaluation, key: string) {
  const value = await request('/v1/evaluations', evaluationJobSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '评估提交结果未知，请核对原提交')
  if (value.profile !== body.profile || value.cutoff !== body.cutoff || value.allow_current_universe !== body.allow_current_universe) throw new ResearchApiError('评估受理内容与原提交不一致', 503, 'invalid_response')
  return value
}
export async function controlEvaluation(identifier: string, action: 'cancel' | 'retry') {
  const value = await request(`/v1/evaluations/${id.parse(identifier)}/${action}`, evaluationJobSchema, { method: 'POST' }, '评估操作结果未知，请刷新原任务状态')
  if (value.id !== identifier) throw new ResearchApiError('评估操作身份不一致', 503, 'invalid_response')
  return value
}
export async function readEvaluationResult(identifier: string, jobId: string, signal?: AbortSignal) {
  const value = await request(`/v1/evaluation-results/${id.parse(identifier)}`, evaluationResultSchema, { signal })
  if (value.id !== identifier || value.job_id !== jobId) throw new ResearchApiError('评估结果不属于原任务', 503, 'invalid_response')
  return value
}
export async function readEvaluationSamples(resultId: string, page: number, strategy?: Strategy, signal?: AbortSignal) {
  const value = await request(`/v1/evaluation-results/${id.parse(resultId)}/samples?page=${page}&page_size=20${strategy ? `&strategy=${strategySchema.parse(strategy)}` : ''}`, evaluationSamplePageSchema, { signal })
  if (value.page !== page || value.page_size !== 20 || value.items.some(sample => sample.result_id !== resultId || strategy && sample.strategy !== strategy)) throw new ResearchApiError('逐股观察与原结果或查询不一致', 503, 'invalid_response')
  return value
}

export async function readSelectionObservations(page: number, signal?: AbortSignal) {
  const value = await request(`/v1/selection-observations?page=${page}&page_size=5`, selectionObservationJobPageSchema, { signal })
  if (value.page !== page || value.page_size !== 5) throw new ResearchApiError('正式观测分页与查询不一致', 503, 'invalid_response')
  return value
}
export async function readSelectionObservation(identifier: string, signal?: AbortSignal) {
  const value = await request(`/v1/selection-observations/${id.parse(identifier)}`, selectionObservationJobSchema, { signal })
  if (value.id !== identifier) throw new ResearchApiError('正式观测任务身份不一致', 503, 'invalid_response')
  return value
}
export async function createSelectionObservation(body: CreateSelectionObservation, key: string) {
  const value = await request('/v1/selection-observations', selectionObservationJobSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '正式观测提交结果未知，请核对原提交')
  if (value.report_id !== body.report_id || value.cutoff !== body.cutoff) throw new ResearchApiError('正式观测受理内容与原提交不一致', 503, 'invalid_response')
  return value
}
export async function controlSelectionObservation(identifier: string, action: 'cancel' | 'retry') {
  const value = await request(`/v1/selection-observations/${id.parse(identifier)}/${action}`, selectionObservationJobSchema, { method: 'POST' }, '正式观测操作结果未知，请刷新原任务状态')
  if (value.id !== identifier) throw new ResearchApiError('正式观测操作身份不一致', 503, 'invalid_response')
  return value
}

export async function readExperiments(page: number, signal?: AbortSignal) {
  const value = await request(`/v1/parameter-experiments?page=${page}&page_size=5`, experimentJobPageSchema, { signal })
  if (value.page !== page || value.page_size !== 5) throw new ResearchApiError('参数实验分页与查询不一致', 503, 'invalid_response')
  return value
}
export async function readExperiment(identifier: string, signal?: AbortSignal) {
  const value = await request(`/v1/parameter-experiments/${id.parse(identifier)}`, experimentJobSchema, { signal })
  if (value.id !== identifier) throw new ResearchApiError('参数实验任务身份不一致', 503, 'invalid_response')
  return value
}
export async function createExperiment(body: CreateExperiment, key: string) {
  const value = await request('/v1/parameter-experiments', experimentJobSchema, { method: 'POST', headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key }, body: JSON.stringify(body) }, '参数实验提交结果未知，请核对原提交')
  if (value.profile !== body.profile || value.cutoff !== body.cutoff || value.field !== body.field || value.values.join() !== body.values.join()) throw new ResearchApiError('参数实验受理内容与原提交不一致', 503, 'invalid_response')
  return value
}
export async function controlExperiment(identifier: string, action: 'cancel' | 'retry') {
  const value = await request(`/v1/parameter-experiments/${id.parse(identifier)}/${action}`, experimentJobSchema, { method: 'POST' }, '参数实验操作结果未知，请刷新原任务状态')
  if (value.id !== identifier) throw new ResearchApiError('参数实验操作身份不一致', 503, 'invalid_response')
  return value
}
export async function readExperimentResult(identifier: string, jobId: string, signal?: AbortSignal) {
  const value = await request(`/v1/parameter-experiment-results/${id.parse(identifier)}`, experimentResultSchema, { signal })
  if (value.id !== identifier || value.job_id !== jobId) throw new ResearchApiError('参数实验结果不属于原任务', 503, 'invalid_response')
  return value
}
export async function readSelectionObservationResult(identifier: string, jobId: string, signal?: AbortSignal) {
  const value = await request(`/v1/selection-observation-results/${id.parse(identifier)}`, selectionObservationResultSchema, { signal })
  if (value.id !== identifier || value.job_id !== jobId) throw new ResearchApiError('正式观测结果不属于原任务', 503, 'invalid_response')
  return value
}
export async function readSelectionObservationSamples(resultId: string, page: number, signal?: AbortSignal) {
  const value = await request(`/v1/selection-observation-results/${id.parse(resultId)}/samples?page=${page}&page_size=20`, selectionObservationSamplePageSchema, { signal })
  if (value.page !== page || value.page_size !== 20 || value.items.some(sample => sample.result_id !== resultId)) throw new ResearchApiError('正式逐股观测与原结果或查询不一致', 503, 'invalid_response')
  return value
}
