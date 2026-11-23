import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { anchorChartSchema } from './anchor-contract'
import { AnchorChartControls, researchAnchorLines } from './anchor-chart'
import { allowedResearchRoute } from '../../../server/stock-research-proxy'

function fixture() {
  const id = '11111111-1111-4111-8111-111111111111'
  return anchorChartSchema.parse({ run_id: id, report_id: id, snapshot_id: id, snapshot_hash: 'a'.repeat(64), as_of: '2026-11-20', target_session: '2026-11-23', company: { symbol: 'AAPL', name: 'Apple' }, calculation_available: true,
    anchors: [{ index: 0, kind: 'swing_low', date: '2026-11-17', confirmed_date: '2026-11-20', daily_estimate: 12.5, hard_filter_enabled: false, ref_prefix: 'martin/AAPL/anchors/0' }],
    selected_index: 0, points: [{ session_date: '2026-11-20', value: 12.5 }], limitations: [] })
}

describe('冻结锚点曲线', () => {
  it('只把后端所选确认后点交给图表，不重算锚点或补零', () => {
    expect(researchAnchorLines(fixture())).toEqual([{ label: '摆动低点 · 日线估算', color: '#8b5cf6', confirmedDate: '2026-11-20', points: [{ date: '2026-11-20', value: 12.5 }] }])
    expect(researchAnchorLines({ ...fixture(), points: [{ session_date: '2026-11-20', value: null }] })[0]!.points).toEqual([])
    expect(researchAnchorLines(undefined)).toEqual([])
  })
  it('拒绝确认前曲线、未来锚点、错误引用和不存在的索引', () => {
    const chart = fixture()
    for (const changed of [
      { ...chart, points: [{ session_date: '2026-11-19', value: 12 }] },
      { ...chart, anchors: [{ ...chart.anchors[0], confirmed_date: '2026-11-23' }] },
      { ...chart, anchors: [{ ...chart.anchors[0], ref_prefix: 'martin/MSFT/anchors/0' }] },
      { ...chart, selected_index: 1 },
      { ...chart, calculation_available: false },
    ]) expect(anchorChartSchema.safeParse(changed).success).toBe(false)
  })
  it('原日期和确认日分开显示，缺计算/空锚点/零量各有说明', () => {
    const render = (chart: ReturnType<typeof fixture>) => renderToStaticMarkup(createElement(AnchorChartControls, { chart, visible: true, onSelect: () => {}, onVisible: () => {} }))
    expect(render(fixture())).toContain('2026-11-17 · 确认 2026-11-20')
    expect(render({ ...fixture(), calculation_available: false, anchors: [], points: [], selected_index: null })).toContain('缺少Martin有效计算')
    expect(render({ ...fixture(), anchors: [], points: [], selected_index: null })).toContain('没有已确认锚点')
    expect(render({ ...fixture(), points: [{ session_date: '2026-11-20', value: null }] })).toContain('没有有效累计成交量')
  })
  it('代理只允许固定报告的锚点读取', () => {
    const id = fixture().report_id
    expect(allowedResearchRoute('GET', `/v1/reports/${id}/anchors/AAPL`)).toBe(true)
    expect(allowedResearchRoute('POST', `/v1/reports/${id}/anchors/AAPL`)).toBe(false)
    expect(allowedResearchRoute('GET', `/v1/reports/${id}/anchors/../env`)).toBe(false)
  })
})
