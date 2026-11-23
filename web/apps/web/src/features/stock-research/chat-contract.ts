import { z } from 'zod'
import { researchHash as hash, researchId as id } from './contract-values'

const point = z.object({ text: z.string().min(1).max(800), evidence_refs: z.array(z.string()).min(1).max(8) })
export const chatAnswerSchema = z.object({ context_hash: hash, answer: point, observations: z.array(point).max(6), risks: z.array(point).max(4), confirmation_conditions: z.array(point).max(4), claims: z.array(z.object({ ref: z.string(), value: z.union([z.string(), z.number().finite(), z.boolean(), z.null()]) })).min(1).max(20) })
const instant = z.string().datetime({ offset: true })
export const chatAttemptSchema = z.object({ number: z.number().int().min(1).max(2), status: z.enum(['running', 'completed', 'failed', 'cancelled']), error_code: z.string().nullable(), tool_calls: z.number().int().nonnegative().nullable(), tool_budget: z.number().int().min(1).max(20), context_hash: hash, deadline: instant, started_time: instant, finished_time: instant.nullable(), receipt_available: z.boolean() }).superRefine((attempt, context) => {
  if ((attempt.status === 'running') !== (attempt.finished_time === null) || (['failed', 'cancelled'].includes(attempt.status)) !== Boolean(attempt.error_code)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答尝试状态不一致' })
  if (attempt.finished_time && Date.parse(attempt.finished_time) < Date.parse(attempt.started_time)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答尝试时间倒退' })
  if (attempt.status === 'completed' && (!attempt.receipt_available || attempt.tool_calls === null || attempt.tool_calls > attempt.tool_budget)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答尝试缺少有效回执' })
})
export const chatTurnSchema = z.object({
  id, conversation_id: id, parent_turn_id: id.nullable(), revision: z.number().int().positive(), report_id: id, question: z.string().min(1).max(6000), status: z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']),
  answer: chatAnswerSchema.nullable(), error_code: z.string().nullable(), model: z.string().nullable(), context_hash: hash.nullable(), source_hash: hash, prompt_hash: hash, tool_calls: z.number().int().nonnegative(), receipt_available: z.boolean(), create_time: z.string().datetime({ offset: true }), update_time: z.string().datetime({ offset: true }),
  attempts: z.array(chatAttemptSchema).max(2).default([]),
}).superRefine((turn, context) => {
  if ((turn.status === 'completed') !== (turn.answer !== null) || (['failed', 'cancelled'].includes(turn.status)) !== Boolean(turn.error_code)) context.addIssue({ code: z.ZodIssueCode.custom, message: '研究问答状态与结果不一致' })
  if ((turn.revision === 1) !== (turn.parent_turn_id === null) || (turn.parent_turn_id === null && turn.conversation_id !== turn.id)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答父上下文不一致' })
  if (turn.status === 'completed' && (!turn.model || !turn.context_hash || !turn.receipt_available || turn.answer?.context_hash !== turn.context_hash)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答完成缺少已验证执行回执' })
  const first = turn.attempts[0], last = turn.attempts.at(-1)
  if (first && last) {
    if (turn.status === 'pending') context.addIssue({ code: z.ZodIssueCode.custom, message: '未开始问答不能已有执行尝试' })
    let spent = 0
    turn.attempts.forEach((attempt, index) => {
      if (attempt.number !== index + 1 || attempt.tool_budget !== 20 - spent || Date.parse(attempt.deadline) !== Date.parse(first.deadline)) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答尝试预算或截止时间不一致' })
      if (index < turn.attempts.length - 1 && (attempt.status !== 'failed' || attempt.tool_calls === null || !['transient_provider_error', 'transport_error'].includes(attempt.error_code || ''))) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答错误不允许重试' })
      spent += attempt.tool_calls ?? 0
    })
    if (spent !== turn.tool_calls || (turn.status !== 'running' && last.status === 'running') || (turn.status === 'completed' && (last.status !== 'completed' || turn.context_hash !== last.context_hash))) context.addIssue({ code: z.ZodIssueCode.custom, message: '问答尝试与最终结果不一致' })
  }
})
export const chatPageSchema = z.object({ items: z.array(chatTurnSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive() })
export type ChatTurn = z.infer<typeof chatTurnSchema>
export type ChatAnswer = z.infer<typeof chatAnswerSchema>
export interface CreateChatTurn { report_id: string; question: string; parent_turn_id?: string }
export const chatIsActive = (turn: ChatTurn | undefined) => turn?.status === 'pending' || turn?.status === 'running'
