import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id, researchQuality } from './contract-values'

export const preparationSchema = z.object({
  id, market: z.literal('us'), target_session: day, as_of: day,
  origin: z.enum(['api', 'cli']), request_id: id.nullable(),
  status: z.enum(['unconfirmed', 'ready', 'failed', 'cancelled']), phase: z.enum(['parameters', 'snapshot', 'analysis', 'freeze']),
  quality: researchQuality.nullable(), error_code: z.enum(['quality_failed', 'input_unavailable', 'source_changed', 'cancelled', 'incomplete']).nullable(), run_id: id.nullable(), source_hash: hash,
  create_time: z.string().datetime({ offset: true }), update_time: z.string().datetime({ offset: true }),
}).superRefine((item, context) => {
  if ((item.origin === 'api') !== (item.request_id !== null) || (item.status === 'ready') !== (item.run_id !== null) || (['failed', 'cancelled'].includes(item.status)) !== (item.error_code !== null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '准备回执状态与引用不一致' })
  if (item.status === 'ready' && (item.phase !== 'freeze' || !item.quality?.passed)) context.addIssue({ code: z.ZodIssueCode.custom, message: '缺少已冻结且通过质量检查的输入' })
})
export const preparationPageSchema = z.object({ items: z.array(preparationSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive() })
export type PreparationAttempt = z.infer<typeof preparationSchema>
