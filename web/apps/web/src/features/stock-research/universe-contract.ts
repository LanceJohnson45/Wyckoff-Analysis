import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id, strategySchema } from './contract-values'

const strategy = z.object({ strategy: strategySchema, data_eligible: z.boolean(), calculation_available: z.boolean(), in_pool: z.boolean(), selected: z.boolean().nullable() })
export const universeStockSchema = z.object({ company: z.object({ symbol: z.string().regex(/^[A-Z][A-Z0-9.-]{0,31}$/), name: z.string().min(1) }), indices: z.array(z.enum(['sp500', 'nasdaq100', 'manual'])), industry: z.string().nullable(), themes: z.array(z.string()), history_rows: z.number().int().nonnegative(), last_session: day.nullable(), strategies: z.array(strategy).length(2), ref_prefix: z.string() }).superRefine((row, context) => {
  if (new Set(row.strategies.map(item => item.strategy)).size !== 2 || Boolean(row.history_rows) !== (row.last_session !== null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '整池策略或行情状态不一致' })
})
export const universePageSchema = z.object({ run_id: id, report_id: id, snapshot_id: id, snapshot_hash: hash, as_of: day, target_session: day, universe_version: z.string(), classification_version: z.string(), universe_total: z.number().int().nonnegative(), items: z.array(universeStockSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive().max(100), search: z.string().nullable(), limitations: z.array(z.string()) }).superRefine((result, context) => {
  if (result.items.length > result.page_size || result.total < result.items.length || result.universe_total < result.total || new Set(result.items.map(item => item.company.symbol)).size !== result.items.length || result.items.some(item => item.last_session && item.last_session > result.as_of)) context.addIssue({ code: z.ZodIssueCode.custom, message: '整池范围、日期或分页不一致' })
})
export type UniverseStock = z.infer<typeof universeStockSchema>
export type UniversePage = z.infer<typeof universePageSchema>
