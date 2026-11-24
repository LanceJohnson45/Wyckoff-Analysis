import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id } from './contract-values'

export const marketRefreshSchema = z.object({
  id, as_of: day, source_hash: hash,
  status: z.enum(['unconfirmed', 'completed', 'failed', 'cancelled']),
  started_at: z.string().datetime({ offset: true }), finished_at: z.string().datetime({ offset: true }).nullable(),
  sync: z.object({
    market: z.literal('us'), rows: z.number().int().nonnegative(), batches: z.number().int().nonnegative(),
    rejected: z.number().int().nonnegative(), issues: z.number().int().nonnegative(),
    checkpoint: z.string().datetime({ offset: true }), scope: z.literal('research:us:all:benchmarks-v1'),
  }).nullable(),
  benchmark_hash: hash.nullable(),
  error_code: z.enum(['refresh_configuration_invalid', 'refresh_source_changed', 'refresh_sync_failed', 'refresh_benchmark_failed', 'refresh_protocol_invalid', 'refresh_process_failed', 'refresh_timeout', 'refresh_cancelled']).nullable(),
}).superRefine((item, context) => {
  if ((item.status === 'unconfirmed') !== (item.finished_at === null) || (['failed', 'cancelled'].includes(item.status)) !== (item.error_code !== null) || (item.status === 'completed' && item.sync === null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '行情更新状态与确认步骤不一致' })
})
export type MarketRefresh = z.infer<typeof marketRefreshSchema>
