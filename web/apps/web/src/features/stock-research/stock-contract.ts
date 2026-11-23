import { z } from 'zod'
import { quantJobSchema } from './quant-contract'
import { researchDay as day, researchHash as hash, researchId as id, strategySchema } from './contract-values'

const scalar = z.union([z.string(), z.number().finite(), z.boolean(), z.null()])
export const stockEvidenceSchema = z.object({ ref: z.string(), symbol: z.string().nullable(), value: scalar })
const bar = z.object({ session_date: day, open: z.number().finite(), high: z.number().finite(), low: z.number().finite(), close: z.number().finite(), volume: z.number().finite().nonnegative(), ref_prefix: z.string() })
export const stockObservationSchema = z.object({
  run_id: id, report_id: id.nullable(), snapshot_id: id, snapshot_hash: hash, as_of: day,
  company: z.object({ symbol: z.string(), name: z.string().min(1) }),
  observations: z.array(z.object({ strategy: strategySchema, artifact_id: id.nullable(), content_hash: hash.nullable(), quant: quantJobSchema.nullable().optional(), parameter_hash: hash, in_pool: z.boolean().nullable(), calculation_available: z.boolean(), facts: z.array(stockEvidenceSchema), exclusions: z.array(stockEvidenceSchema), limitations: z.array(z.string()) })),
  history: z.array(bar).max(320), history_total: z.number().int().nonnegative(),
  groups: z.array(z.object({ name: z.string(), members: z.array(z.string()), valid_members: z.array(z.string()), coverage: z.number().min(0).max(1), usable: z.boolean(), rs_median: z.number().finite().nullable(), up_1: z.number().finite().nullable(), up_3: z.number().finite().nullable(), trend_health: z.number().finite().nullable(), score: z.number().finite().nullable(), ref_prefix: z.string() })).nullable(),
  limitations: z.array(z.string()),
}).superRefine((stock, context) => {
  if (stock.observations.some(row => row.artifact_id === null && (!row.quant || row.quant.status === 'completed' || row.content_hash !== null || row.in_pool !== null || row.calculation_available || row.facts.length > 0))) context.addIssue({ code: z.ZodIssueCode.custom, message: '缺量化不能冒充空产物' })
  const dates = stock.history.map(row => row.session_date)
  if (dates.some((date, index) => date > stock.as_of || (index > 0 && date <= dates[index - 1]!)) || stock.history_total < dates.length) context.addIssue({ code: z.ZodIssueCode.custom, message: '历史行情超出截止日或顺序异常' })
  if (stock.observations.length !== 2 || new Set(stock.observations.map(row => row.strategy)).size !== 2) context.addIssue({ code: z.ZodIssueCode.custom, message: '策略产物缺失或重复' })
})
export type StockObservation = z.infer<typeof stockObservationSchema>
export type StockEvidence = z.infer<typeof stockEvidenceSchema>
