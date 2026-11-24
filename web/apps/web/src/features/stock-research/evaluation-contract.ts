import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id, strategySchema } from './contract-values'

export const evaluationJobSchema = z.object({
  id, kind: z.literal('historical_quant_pool'), profile: z.string().min(1), cutoff: day,
  allow_current_universe: z.boolean(), status: z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']),
  completed_sections: z.number().int().min(0).max(20), error_code: z.string().nullable(), result_id: id.nullable(),
  cancel_requested: z.boolean(), attempt_count: z.number().int().min(0).max(2),
  create_time: z.string().datetime({ offset: true }), update_time: z.string().datetime({ offset: true }),
}).superRefine((job, context) => {
  if (job.status === 'completed' ? !job.result_id || job.completed_sections !== 20 || job.error_code !== null : job.result_id !== null) context.addIssue({ code: 'custom', message: '评估完成状态与结果不一致' })
})

export const evaluationJobPageSchema = z.object({ items: z.array(evaluationJobSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().min(1).max(100) })
export const horizonSchema = z.union([z.literal(1), z.literal(3), z.literal(5), z.literal(10), z.literal(20)])
const metric = z.number().finite().nullable()
export const outcomeWindowSchema = z.object({
  horizon: horizonSchema, target_session: day, status: z.enum(['completed', 'pending', 'missing_data']),
  missing_sessions: z.array(z.string()), gross_return_pct: metric, cost_pct: z.number().finite().nonnegative(),
  net_return_pct: metric, benchmark_return_pct: metric, net_excess_pct: metric, mfe_pct: metric, mae_pct: metric,
}).superRefine((window, context) => {
  const values = [window.gross_return_pct, window.net_return_pct, window.benchmark_return_pct, window.net_excess_pct, window.mfe_pct, window.mae_pct]
  if (window.status === 'completed' ? values.some(value => value === null) : values.some(value => value !== null)) context.addIssue({ code: 'custom', message: '观察窗口状态与数值不一致' })
})

const partition = z.enum(['research', 'validation', 'sealed'])
const section = z.object({ as_of: day, spy_return_20_pct: z.number().finite(), environment_rank: z.number().int().nonnegative(), partition, label_end: day, purged: z.boolean() })
const sectionSummary = z.object({
  section, strategy: strategySchema, snapshot_hash: hash, parameter_hash: hash, analysis_hash: hash,
  universe_count: z.number().int().nonnegative(), eligible_count: z.number().int().nonnegative(), observation_count: z.number().int().nonnegative(),
  exclusions: z.record(z.number().int().nonnegative()), pool: z.array(z.string()),
})
export const evaluationResultSchema = z.object({
  id, job_id: id, kind: z.literal('historical_quant_pool'), report_hash: hash, input_hash: hash, source_hash: hash,
  parameter_hashes: z.object({ martin: hash, wyckoff: hash }), cutoff: day, allow_current_universe: z.boolean(),
  candidate_count: z.number().int().min(20), usable_sections: z.record(z.number().int().min(0).max(20)),
  sections: z.array(sectionSummary).length(40), model_selection: z.null(), manual_review: z.literal('pending'), limitations: z.array(z.string()),
})

export const evaluationSampleSchema = z.object({
  id, result_id: id, as_of: day, strategy: strategySchema, symbol: z.string(), partition, purged: z.boolean(),
  status: z.enum(['completed', 'pending', 'missing_data', 'sealed']), windows: z.array(outcomeWindowSchema).nullable(),
}).superRefine((sample, context) => {
  if (sample.partition === 'sealed' ? sample.status !== 'sealed' || sample.windows !== null : sample.status === 'sealed' || sample.windows?.map(window => window.horizon).join(',') !== '1,3,5,10,20') context.addIssue({ code: 'custom', message: '封存状态或观察窗口不完整' })
})
export const evaluationSamplePageSchema = z.object({ items: z.array(evaluationSampleSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().min(1).max(100) })

export type EvaluationJob = z.infer<typeof evaluationJobSchema>
export type EvaluationResult = z.infer<typeof evaluationResultSchema>
export type EvaluationSample = z.infer<typeof evaluationSampleSchema>
export type EvaluationHorizon = z.infer<typeof horizonSchema>
export interface CreateEvaluation { profile: string; cutoff: string; allow_current_universe: boolean }
export function evaluationActive(job: EvaluationJob): boolean { return job.status === 'pending' || job.status === 'running' }
