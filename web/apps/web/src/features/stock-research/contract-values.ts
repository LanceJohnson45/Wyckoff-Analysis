import { z } from 'zod'

export const strategySchema = z.enum(['martin', 'wyckoff'])
export const researchId = z.string().uuid()
export const researchDay = z.string().regex(/^\d{4}-\d{2}-\d{2}$/)
export const researchHash = z.string().regex(/^[a-f0-9]{64}$/)
export const researchQuality = z.object({ passed: z.boolean(), coverage: z.number().min(0).max(1), aligned_count: z.number().int(), universe_count: z.number().int(), issues: z.array(z.object({ symbol: z.string(), reason: z.string(), strategy: z.string() })), limitations: z.array(z.string()) })

export type ResearchJson = string | number | boolean | null | ResearchJson[] | { [key: string]: ResearchJson }
export const researchJson: z.ZodType<ResearchJson> = z.lazy(() => z.union([
  z.string(), z.number().finite(), z.boolean(), z.null(), z.array(researchJson), z.record(researchJson),
]))
