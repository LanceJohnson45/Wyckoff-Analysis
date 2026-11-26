import { z } from 'zod'
import { researchDay as day, researchId as id } from './contract-values'

const timestamp = z.string().datetime({ offset: true })
const phase = z.object({
  state: z.enum(['not_recorded', 'preparing', 'ready', 'sending', 'retry', 'unknown', 'failed', 'silent', 'sent', 'unconfirmed']),
  ledger_update_time: timestamp.nullable(), has_native_receipt: z.boolean(),
}).superRefine((value, context) => {
  if (value.state === 'not_recorded' ? value.ledger_update_time !== null || value.has_native_receipt : value.ledger_update_time === null || value.state === 'sent' && !value.has_native_receipt) context.addIssue({ code: 'custom', message: '通知状态与时间或原生回执不一致' })
})
const delivery = z.object({
  target_session: day, checked_at: timestamp, initial: phase, recovery: phase,
  latest_sent: z.object({ target_session: day, phase: z.enum(['initial', 'recovery']), ledger_update_time: timestamp }).nullable(),
})
export const operationsSchema = z.object({
  target_session: day, checked_at: timestamp,
  latest_completed: z.object({ run_id: id, target_session: day, update_time: timestamp }).nullable(),
  delivery: delivery.nullable(),
}).superRefine((value, context) => {
  if (value.delivery && value.delivery.target_session !== value.target_session) context.addIssue({ code: 'custom', message: '通知与当前目标日不一致' })
})
export type OperationsView = z.infer<typeof operationsSchema>
