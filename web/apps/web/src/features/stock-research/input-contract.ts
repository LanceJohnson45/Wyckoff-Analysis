import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id } from './contract-values'

const text = z.string().trim().min(1).max(512)
const symbol = z.string().regex(/^[A-Z][A-Z0-9]{0,9}(?:-[A-Z0-9]{1,4})?$/)
export const themeDefinitionSchema = z.object({ name: text.max(128), members: z.array(symbol).max(2000), source: text, reason: z.string().trim().min(1).max(3000) })
const manualDefinitionSchema = z.object({ symbol, action: z.enum(['add', 'remove']), name: text.nullable(), industry: text.nullable(), source: text, reason: z.string().trim().min(1).max(3000) })
export const inputPublicationSchema = z.object({ id, kind: z.enum(['theme', 'manual']), target: text, revision: z.number().int().positive(), previous_hash: hash.nullable(), proposal_id: id.nullable(), effective_date: day, confirmed_time: z.string().datetime({ offset: true }), definition: z.union([themeDefinitionSchema, manualDefinitionSchema]), content_hash: hash }).superRefine((row, context) => {
  const theme = 'members' in row.definition
  if ((row.kind === 'theme') !== theme || theme !== Boolean(row.proposal_id) || (row.revision === 1) !== (row.previous_hash === null) || row.target !== ('members' in row.definition ? row.definition.name : row.definition.symbol)) context.addIssue({ code: z.ZodIssueCode.custom, message: '人工发布关联或修订不一致' })
})
export const themeProposalSchema = z.object({ id, definition: themeDefinitionSchema, base_revision: z.number().int().nonnegative(), base_universe_hash: hash, base_classification_hash: hash, previous_members: z.array(symbol), current_revision: z.number().int().nonnegative(), status: z.enum(['pending', 'confirmed']), publication: inputPublicationSchema.nullable(), create_time: z.string().datetime({ offset: true }) }).superRefine((row, context) => {
  if ((row.status === 'confirmed') !== Boolean(row.publication) || (row.publication && (row.publication.proposal_id !== row.id || row.publication.revision !== row.base_revision + 1 || JSON.stringify(row.publication.definition) !== JSON.stringify(row.definition)))) context.addIssue({ code: z.ZodIssueCode.custom, message: '提议与人工确认版本不一致' })
})
const paging = { total: z.number().int().nonnegative(), page: z.number().int().positive(), page_size: z.number().int().positive().max(20) }
export const themeProposalPageSchema = z.object({ items: z.array(themeProposalSchema), ...paging })
export const inputPublicationPageSchema = z.object({ items: z.array(inputPublicationSchema), ...paging })
export const manualHeadSchema = z.object({ symbol, revision: z.number().int().nonnegative(), publication: inputPublicationSchema.nullable() }).superRefine((row, context) => {
  if ((row.revision === 0) !== (row.publication === null) || (row.publication && (row.publication.kind !== 'manual' || row.publication.target !== row.symbol || row.publication.revision !== row.revision))) context.addIssue({ code: z.ZodIssueCode.custom, message: '自选当前修订不一致' })
})
export const frozenInputsSchema = z.object({ run_id: id, inputs: z.object({ source: z.enum(['operator_baseline', 'published', 'frozen_snapshot']), as_of: day, universe_version: z.string(), classification_version: z.string(), universe_hash: hash, classification_hash: hash, base_universe_hash: hash.nullable(), base_classification_hash: hash.nullable(), publications: z.array(inputPublicationSchema) }) }).superRefine((row, context) => {
  if ((row.inputs.source === 'published') !== (row.inputs.publications.length > 0) || row.inputs.publications.some(publication => publication.effective_date > row.inputs.as_of)) context.addIssue({ code: z.ZodIssueCode.custom, message: '冻结来源包含未来发布或不一致配置' })
})
export type ThemeDefinition = z.infer<typeof themeDefinitionSchema>
export type ThemeProposal = z.infer<typeof themeProposalSchema>
export type InputPublication = z.infer<typeof inputPublicationSchema>
export type ManualHead = z.infer<typeof manualHeadSchema>
export type ConfirmTheme = { expected_revision: number; effective_date: string }
export type CreateManualOverride = z.infer<typeof manualDefinitionSchema> & ConfirmTheme

export function canonicalResearchSymbol(value: string): string { return value.trim().toUpperCase().replace(/^US:/, '').replace(/\.US$/, '').replaceAll('.', '-') }
