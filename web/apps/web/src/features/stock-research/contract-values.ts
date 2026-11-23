import { z } from 'zod'

export const strategySchema = z.enum(['martin', 'wyckoff'])
export const researchId = z.string().uuid()
export const researchDay = z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
export const researchHash = z.string().regex(/^[a-f0-9]{64}$/)

export type ResearchJson = string | number | boolean | null | ResearchJson[] | { [key: string]: ResearchJson }
export const researchJson: z.ZodType<ResearchJson> = z.lazy(() => z.union([
  z.string(), z.number().finite(), z.boolean(), z.null(), z.array(researchJson), z.record(researchJson),
]))
