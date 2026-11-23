import { z } from 'zod'
import { researchHash as hash, researchId as id } from './contract-values'

const point = z.object({ text: z.string().min(1).max(800), evidence_refs: z.array(z.string()).min(1).max(8) })
export const chatAnswerSchema = z.object({ context_hash: hash, answer: point, observations: z.array(point).max(6), risks: z.array(point).max(4), confirmation_conditions: z.array(point).max(4), claims: z.array(z.object({ ref: z.string(), value: z.union([z.string(), z.number().finite(), z.boolean(), z.null()]) })).min(1).max(20) })
export const chatTurnSchema = z.object({
  id, conversation_id: id, parent_turn_id: id.nullable(), revision: z.number().int().positive(), report_id: id, question: z.string().min(1).max(6000), status: z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']),
  answer: chatAnswerSchema.nullable(), error_code: z.string().nullable(), model: z.string().nullable(), context_hash: hash.nullable(), source_hash: hash, prompt_hash: hash, tool_calls: z.number().int().nonnegative(), receipt_available: z.boolean(), create_time: z.string().datetime({ offset: true }), update_time: z.string().datetime({ offset: true }),
}).superRefine((turn, context) => {
  if ((turn.status === 'completed') !== (turn.answer !== null) || (['failed', 'cancelled'].includes(turn.status)) !== Boolean(turn.error_code)) context.addIssue({ code: z.ZodIssueCode.custom, message: '研究问答状态与结果不一致' })
  if ((turn.revision === 1) !== (turn.parent_turn_id === null) || (turn.parent_turn_id === null && turn.conversation_id !== turn.id)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答父上下文不一致' })
  if (turn.status === 'completed' && (!turn.model || !turn.context_hash || !turn.receipt_available || turn.answer?.context_hash !== turn.context_hash)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答完成缺少已验证执行回执' })
})
export const chatPageSchema = z.object({ items: z.array(chatTurnSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive() })
export type ChatTurn = z.infer<typeof chatTurnSchema>
export type ChatAnswer = z.infer<typeof chatAnswerSchema>
export interface CreateChatTurn { report_id: string; question: string; parent_turn_id?: string }
export const chatIsActive = (turn: ChatTurn | undefined) => turn?.status === 'pending' || turn?.status === 'running'
