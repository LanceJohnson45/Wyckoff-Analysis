import { z } from 'zod'
import { researchId as id, researchHash as hash, strategySchema } from './contract-values'

export const quantJobSchema = z.object({
  id: id.nullable(), strategy: strategySchema, status: z.enum(['pending', 'running', 'completed', 'failed', 'cancelled']),
  attempt_count: z.number().int().nonnegative().nullable(), artifact_id: id.nullable(), analysis_hash: hash.nullable(), error_code: z.string().nullable(),
}).superRefine((row, context) => {
  if (row.id !== null && ((row.status === 'pending' && row.attempt_count !== 0) || (row.status !== 'pending' && (row.attempt_count === null || row.attempt_count < 1)))) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化状态与尝试次数不一致' })
  if ((['failed', 'cancelled'].includes(row.status) && !row.error_code) || (['pending', 'running', 'completed'].includes(row.status) && row.error_code !== null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化错误与状态不一致' })
  if (row.status !== 'completed' && (row.artifact_id !== null || row.analysis_hash !== null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '失败或未完成量化不得携带产物' })
  if ((row.status === 'completed') !== (row.artifact_id !== null && row.analysis_hash !== null) || (row.id === null && (row.status !== 'completed' || row.attempt_count !== null)) || (row.id !== null && row.attempt_count === null)) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化状态与成功产物不一致' })
})
export type QuantJob = z.infer<typeof quantJobSchema>

const quantAttemptSchema = z.object({ id, job_id: id, strategy: strategySchema, status: z.enum(['running', 'completed', 'failed', 'cancelled']), attempt_count: z.number().int().positive(), artifact_id: id.nullable(), analysis_hash: hash.nullable(), error_code: z.string().nullable(), create_time: z.string().datetime({ offset: true }), update_time: z.string().datetime({ offset: true }) })
export const quantAttemptPageSchema = z.object({ run_id: id, recorded: z.boolean(), items: z.array(quantAttemptSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive().max(20) }).superRefine((page, context) => {
  if (page.items.some(row => !quantJobSchema.safeParse(row).success)) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化尝试终态无效' })
  if (page.items.length > page.page_size || page.total < page.items.length || (!page.recorded && page.total > 0)) context.addIssue({ code: z.ZodIssueCode.custom, message: '量化尝试分页来源无效' })
})

const quantErrors: Record<string, string> = { quant_unavailable: '部分策略量化尚未完成', quant_calculation_failed: '量化计算失败', quant_timeout: '量化计算超时', quant_process_failed: '量化进程中断', quant_output_invalid: '量化结果校验失败', quant_source_changed: '量化源码版本已变化', previous_executor_stopped: '上次执行中断', cancelled: '已取消' }
export function quantFailureLabel(code: string): string | undefined { return quantErrors[code] }

export const quantStatusLabels = { pending: '待开始', running: '计算中', completed: '成功', failed: '失败', cancelled: '已取消' }
