import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { universePageSchema } from './universe-contract'
import { UniverseTable } from './universe'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

const ID = '11111111-1111-4111-8111-111111111111'
const REPORT = '22222222-2222-4222-8222-222222222222'
const row = { company: { symbol: 'LOST', name: '<img src=x onerror=alert(1)>' }, indices: ['sp500', 'nasdaq100'], industry: null, themes: [], history_rows: 0, last_session: null, strategies: [{ strategy: 'martin', data_eligible: false, calculation_available: false, in_pool: false, selected: null }, { strategy: 'wyckoff', data_eligible: false, calculation_available: false, in_pool: false, selected: false }], ref_prefix: 'universe/LOST' }
const page = { run_id: ID, report_id: REPORT, snapshot_id: ID, snapshot_hash: 'a'.repeat(64), as_of: '2026-11-20', target_session: '2026-11-23', universe_version: 'fixture', classification_version: 'fixture', universe_total: 518, items: [row], total: 1, page: 1, page_size: 20, search: 'LOST', limitations: ['合成接口样本'] }

describe('冻结整池浏览', () => {
  it('缺行情/分类保留，未提交与未正式入选分别表达，外部公司名转义', () => {
    const data = universePageSchema.parse(page)
    const html = renderToStaticMarkup(createElement(UniverseTable, { data, selected: 'LOST', onSelect: () => undefined }))
    expect(html).toContain('无可用行情')
    expect(html).toContain('未取得行业分类')
    expect(html).toContain('未计算')
    expect(html).toContain('初选未提交')
    expect(html).toContain('未正式入选')
    expect(html).toContain('aria-pressed="true"')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
  })
  it('拒绝未来日期、缺失或重复策略及不一致分页', () => {
    expect(universePageSchema.safeParse({ ...page, items: [{ ...row, history_rows: 1 }] }).success).toBe(false)
    expect(universePageSchema.safeParse({ ...page, items: [{ ...row, history_rows: 1, last_session: '2026-11-23' }] }).success).toBe(false)
    expect(universePageSchema.safeParse({ ...page, items: [{ ...row, strategies: [row.strategies[0], row.strategies[0]] }] }).success).toBe(false)
    expect(universePageSchema.safeParse({ ...page, items: [row, row], total: 2 }).success).toBe(false)
    expect(universePageSchema.safeParse({ ...page, universe_total: 0 }).success).toBe(false)
  })
  it('空搜索结果独立表达，代理只开放GET整池', () => {
    const data = universePageSchema.parse({ ...page, items: [], total: 0 })
    const html = renderToStaticMarkup(createElement(UniverseTable, { data, selected: '', onSelect: () => undefined }))
    expect(html).toContain('没有匹配股票')
    expect(html).not.toContain('<table')
    expect(allowedResearchRoute('GET', `/v1/reports/${REPORT}/universe`)).toBe(true)
    expect(allowedResearchRoute('POST', `/v1/reports/${REPORT}/universe`)).toBe(false)
  })
})
