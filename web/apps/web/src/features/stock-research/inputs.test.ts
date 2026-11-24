import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { inputPublicationSchema, themeProposalSchema, manualHeadSchema, frozenInputsSchema } from './input-contract'
import { ThemeProposalCard, ThemeReview } from './input-theme'
import { PublicationCard } from './inputs'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const ID = '11111111-1111-4111-8111-111111111111', PUB = '22222222-2222-4222-8222-222222222222'
const definition = { name: '<script>theme</script>', members: ['AAPL'], source: '<img src=x onerror=bad()>', reason: '人工复核理由' }
const publication = { id: PUB, kind: 'theme', target: definition.name, revision: 1, previous_hash: null, proposal_id: ID, effective_date: '2026-11-24', confirmed_time: '2026-11-23T12:00:00+08:00', definition, content_hash: 'b'.repeat(64) }
const proposal = { id: ID, definition, base_revision: 0, base_universe_hash: 'a'.repeat(64), base_classification_hash: 'b'.repeat(64), previous_members: ['MSFT'], current_revision: 0, status: 'pending', publication: null, create_time: '2026-11-23T12:00:00+08:00' }
const actions = { submit: async () => true, busy: false, uncertain: false, blocked: false, message: '', showMessage: () => undefined }

describe('人工输入维护', () => {
  it('待确认与已确认须有真实发布关联，拒绝错对象及未来冻结来源', () => {
    expect(themeProposalSchema.safeParse(proposal).success).toBe(true)
    expect(themeProposalSchema.safeParse({ ...proposal, status: 'confirmed' }).success).toBe(false)
    expect(themeProposalSchema.safeParse({ ...proposal, status: 'confirmed', publication }).success).toBe(true)
    expect(inputPublicationSchema.safeParse({ ...publication, kind: 'manual' }).success).toBe(false)
    expect(manualHeadSchema.safeParse({ symbol: 'AAPL', revision: 1, publication: null }).success).toBe(false)
    expect(frozenInputsSchema.safeParse({ run_id: ID, inputs: { source: 'published', as_of: '2026-11-20', universe_version: 'fixture', classification_version: 'fixture', universe_hash: 'a'.repeat(64), classification_hash: 'b'.repeat(64), base_universe_hash: null, base_classification_hash: null, publications: [publication] } }).success).toBe(false)
  })
  it('人工复核完整成员与增删差异，外部主题和来源文字转义', () => {
    const row = themeProposalSchema.parse(proposal)
    const html = renderToStaticMarkup(createElement(ThemeReview, { proposal: row, actions, onUpdate: () => undefined, onClose: () => undefined }))
    expect(html).toContain('原完整成员：MSFT')
    expect(html).toContain('新完整成员：AAPL')
    expect(html).toContain('加入：AAPL')
    expect(html).toContain('移除：MSFT')
    expect(html).toContain('确认并发布此主题')
    expect(html).toContain('&lt;script&gt;')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<script>theme')
  })
  it('基准变化阻止旧提议覆盖，已确认版本显示明确生效日', () => {
    const stale = themeProposalSchema.parse({ ...proposal, current_revision: 1 })
    const html = renderToStaticMarkup(createElement(ThemeReview, { proposal: stale, actions, onUpdate: () => undefined, onClose: () => undefined }))
    expect(html).toContain('已有新发布')
    expect(html).toMatch(/disabled="">确认并发布此主题/)
    const card = renderToStaticMarkup(createElement(ThemeProposalCard, { proposal: themeProposalSchema.parse({ ...proposal, current_revision: 1, status: 'confirmed', publication }), onReview: () => undefined, disabled: false }))
    expect(card).toContain('已人工确认')
    expect(card).toContain('生效日 2026-11-24')
    expect(renderToStaticMarkup(createElement(PublicationCard, { value: inputPublicationSchema.parse(publication) }))).toContain('确认')
  })
  it.each([1, 2])('已确认建议在当前修订 %i 下保留确认结果，不再提示未确认冲突', (current_revision) => {
    const row = themeProposalSchema.parse({ ...proposal, current_revision, status: 'confirmed', publication })
    const html = renderToStaticMarkup(createElement(ThemeReview, { proposal: row, actions, onUpdate: () => undefined, onClose: () => undefined }))
    expect(html).toContain('已确认修订 1，生效日 2026-11-24')
    expect(html).not.toContain('已有新发布，当前建议不能覆盖')
    expect(html).not.toContain('确认并发布此主题')
  })
  it('代理开放固定人工入口，不提供建议覆盖/自动确认或任意路径', () => {
    for (const path of ['/v1/theme-proposals', `/v1/theme-proposals/${ID}`, '/v1/input-publications', '/v1/universe-overrides', '/v1/universe-overrides/AAPL', `/v1/research-runs/${ID}/inputs`]) expect(allowedResearchRoute('GET', path)).toBe(true)
    expect(allowedResearchRoute('POST', `/v1/theme-proposals/${ID}/confirm`)).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/universe-overrides')).toBe(true)
    expect(allowedResearchRoute('PUT', `/v1/theme-proposals/${ID}`)).toBe(false)
    expect(allowedResearchRoute('POST', '/v1/theme-proposals/auto-confirm')).toBe(false)
  })
})
