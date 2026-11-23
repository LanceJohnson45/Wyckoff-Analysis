import { z } from 'zod'
import { strategySchema, researchId as id, researchDay as day } from './contract-values'

const state = z.enum(['new', 'waiting', 'near', 'invalid'])
export const watchItemSchema = z.object({
  symbol: z.string().min(1), name: z.string().min(1), strategy: strategySchema, state,
  target_session: day, checked_run_id: id, state_run_id: id, state_snapshot_id: id, state_as_of: day, state_session: day,
  data_insufficient: z.boolean(), not_reviewed: z.boolean(), reason: z.string(), evidence_refs: z.array(z.string()), policy_version: z.literal('watch-v1'),
})
export const watchPageSchema = z.object({ items: z.array(watchItemSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive() })
export const watchEventPageSchema = z.object({
  items: z.array(z.object({ id, run_id: id, target_session: day, strategy: strategySchema, symbol: z.string(), kind: z.string(), before: state.nullable(), after: watchItemSchema })),
  total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive(),
})
export type ResearchWatchItem = z.infer<typeof watchItemSchema>
