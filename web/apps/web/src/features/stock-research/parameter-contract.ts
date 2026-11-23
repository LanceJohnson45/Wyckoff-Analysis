import { z } from 'zod'
import { researchHash as hash, researchId as id, researchJson, strategySchema } from './contract-values'

export const parameterValuesSchema = z.object({ version: z.string().min(1).max(256).refine(value => !value.includes('\u0000')), algorithm: z.enum(['martin-daily-v1', 'production-us-l1-l4-v1']) }).catchall(researchJson)
const model = z.object({ strategy: strategySchema, parameters: parameterValuesSchema })
const matchesStrategy = (row: z.infer<typeof model>) => row.parameters.algorithm === (row.strategy === 'martin' ? 'martin-daily-v1' : 'production-us-l1-l4-v1')
export const parameterSetSchema = model.extend({ id, parameter_hash: hash, create_time: z.string().datetime({ offset: true }) }).refine(matchesStrategy, '参数算法与策略不一致')
export const strategyParameterSchema = model.extend({ source: z.enum(['operator_baseline', 'published']), revision: z.number().int().nonnegative(), parameter_set_id: id.nullable(), parameter_hash: hash }).refine(matchesStrategy, '参数算法与策略不一致').refine(row => row.source === 'published' ? row.revision > 0 && row.parameter_set_id !== null : row.revision === 0 && row.parameter_set_id === null, '参数来源与修订不一致')
export const strategyParametersSchema = z.object({ items: z.array(strategyParameterSchema).length(2) }).refine(value => new Set(value.items.map(item => item.strategy)).size === 2, '策略配置缺失或重复')
export const parameterPageSchema = z.object({ items: z.array(parameterSetSchema), total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive() })
export type ParameterValues = z.infer<typeof parameterValuesSchema>
export type ParameterSet = z.infer<typeof parameterSetSchema>
export type StrategyParameters = z.infer<typeof strategyParameterSchema>

export function parseParameterDraft(text: string, strategy: z.infer<typeof strategySchema>): ParameterValues {
  let raw: unknown
  try { raw = JSON.parse(text) }
  catch { throw new Error('参数JSON无效，请检查括号、引号与逗号') }
  const parsed = model.safeParse({ strategy, parameters: raw })
  if (!parsed.success || !matchesStrategy(parsed.data)) throw new Error('请填写版本名并保留本策略的算法与参数结构')
  return parsed.data.parameters
}
