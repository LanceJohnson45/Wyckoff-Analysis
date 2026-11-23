import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { parseParameterDraft, strategyParameterSchema, strategyParametersSchema } from './parameter-contract'
import { ParameterSummary } from './parameters'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const ID = '11111111-1111-4111-8111-111111111111'
const martin = { strategy: 'martin', parameters: { version: 'baseline-v1', algorithm: 'martin-daily-v1', support_atr: 0.75, rules: { support: { enabled: true, action: 'exclude' } } }, source: 'operator_baseline', revision: 0, parameter_set_id: null, parameter_hash: 'a'.repeat(64) }

describe('研究参数维护契约', () => {
  it('编辑往返保留嵌套规则与数值，不用默认值修补错误草稿', () => {
    expect(parseParameterDraft(JSON.stringify(martin.parameters), 'martin')).toEqual(martin.parameters)
    expect(() => parseParameterDraft('{', 'martin')).toThrow('JSON无效')
    expect(() => parseParameterDraft(JSON.stringify(martin.parameters), 'wyckoff')).toThrow('算法')
    expect(() => parseParameterDraft(JSON.stringify({ ...martin.parameters, version: '' }), 'martin')).toThrow('版本名')
    expect(() => parseParameterDraft(JSON.stringify({ ...martin.parameters, version: 'bad\u0000name' }), 'martin')).toThrow('版本名')
  })
  it('缺策略、重复策略或发布来源与修订不一致时失败', () => {
    const wyckoff = { ...martin, strategy: 'wyckoff', parameters: { version: 'baseline-v1', algorithm: 'production-us-l1-l4-v1' } }
    expect(strategyParametersSchema.safeParse({ items: [martin, wyckoff] }).success).toBe(true)
    expect(strategyParametersSchema.safeParse({ items: [martin] }).success).toBe(false)
    expect(strategyParametersSchema.safeParse({ items: [martin, martin] }).success).toBe(false)
    expect(strategyParameterSchema.safeParse({ ...martin, source: 'published', revision: 1 }).success).toBe(false)
    expect(strategyParameterSchema.safeParse({ ...martin, revision: 1, parameter_set_id: ID }).success).toBe(false)
    expect(strategyParameterSchema.safeParse({ ...martin, source: 'published', revision: 1, parameter_set_id: ID }).success).toBe(true)
  })
  it('冻结配置明确标记来源与修订，并转义用户版本名', () => {
    const item = strategyParameterSchema.parse({ ...martin, parameters: { ...martin.parameters, version: '<img src=x onerror=alert(1)>' } })
    const html = renderToStaticMarkup(createElement(ParameterSummary, { item, frozen: true }))
    expect(html).toContain('运行冻结')
    expect(html).toContain('Operator基线')
    expect(html).toContain('修订 0')
    expect(html).toContain('support_atr')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
  })
  it('代理只开放显式登记和两策略发布，不开放覆盖或删除', () => {
    expect(allowedResearchRoute('GET', '/v1/strategies')).toBe(true)
    expect(allowedResearchRoute('GET', `/v1/research-runs/${ID}/parameters`)).toBe(true)
    expect(allowedResearchRoute('GET', `/v1/parameter-sets/${ID}`)).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/parameter-sets')).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/strategies/wyckoff/activate-parameters')).toBe(true)
    expect(allowedResearchRoute('POST', '/v1/strategies/other/activate-parameters')).toBe(false)
    expect(allowedResearchRoute('POST', `/v1/parameter-sets/${ID}`)).toBe(false)
    expect(allowedResearchRoute('PUT', '/v1/parameter-sets')).toBe(false)
    expect(allowedResearchRoute('DELETE', `/v1/parameter-sets/${ID}`)).toBe(false)
  })
})
