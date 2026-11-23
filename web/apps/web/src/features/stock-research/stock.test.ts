import { createElement } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'
import { stockObservationSchema, type StockObservation } from './stock-contract'
import { researchKlines, StockFactsView } from './stock'
import { resolveChartOverlays } from '@/components/kline-chart'

const RUN = '11111111-1111-4111-8111-111111111111'
const REPORT = '22222222-2222-4222-8222-222222222222'
function fixture(): StockObservation {
  return stockObservationSchema.parse({
    run_id: RUN, report_id: REPORT, snapshot_id: RUN, snapshot_hash: 'a'.repeat(64), as_of: '2026-11-20', company: { symbol: 'AAPL', name: 'Apple' },
    history: [{ session_date: '2026-11-20', open: 10, high: 12, low: 9, close: 11, volume: 1000, ref_prefix: 'bar/stock/AAPL/2026-11-20' }], history_total: 1, groups: [], limitations: [],
    observations: [
      { strategy: 'martin', artifact_id: RUN, content_hash: 'b'.repeat(64), parameter_hash: 'c'.repeat(64), in_pool: false, calculation_available: true, facts: [{ ref: 'martin/AAPL/factors/close', symbol: 'AAPL', value: 11 }, { ref: 'martin/AAPL/factors/dollar_volume_proxy', symbol: 'AAPL', value: null }], exclusions: [], limitations: ['合成接口样本'] },
      { strategy: 'wyckoff', artifact_id: REPORT, content_hash: 'd'.repeat(64), parameter_hash: 'e'.repeat(64), in_pool: false, calculation_available: false, facts: [], exclusions: [], limitations: ['历史不足'] },
    ],
  })
}

describe('固定股票证据', () => {
  it('图表直接使用快照OHLCV，不补日期或重算价格', () => {
    expect(researchKlines(fixture())).toEqual([{ date: '2026-11-20', open: 10, high: 12, low: 9, close: 11, volume: 1000 }])
  })
  it('纯行情模式不向图表提供自动指标、结构标记或支撑压力', () => {
    const candles = researchKlines(fixture())
    expect(resolveChartOverlays(candles, true)).toEqual({ ma5: [], ma20: [], ma50: [], ma200: [], markers: [], levels: null })
    expect(resolveChartOverlays(candles, false).levels).toEqual({ support: 9, resistance: 12 })
  })
  it('拒绝未来/重复日线及策略缺失', () => {
    const stock = fixture(), last = stock.history[0]!
    expect(stockObservationSchema.safeParse({ ...stock, history: [{ ...last, session_date: '2026-11-23' }] }).success).toBe(false)
    expect(stockObservationSchema.safeParse({ ...stock, history: [last, last], history_total: 2 }).success).toBe(false)
    expect(stockObservationSchema.safeParse({ ...stock, observations: stock.observations.slice(0, 1) }).success).toBe(false)
    expect(stockObservationSchema.safeParse({ ...stock, observations: [...stock.observations, stock.observations[0]] }).success).toBe(false)
  })
  it('缺行情不画图，缺计算不渲染成零，外部文字转义', () => {
    const stock = { ...fixture(), history: [], company: { symbol: 'AAPL', name: '<img src=x onerror=alert(1)>' } }
    const html = renderToStaticMarkup(createElement(StockFactsView, { stock }))
    expect(html).toContain('没有本次可用日线')
    expect(html).toContain('缺少有效量化计算')
    expect(html).toContain('缺失')
    expect(html).toContain('未进入量化池')
    expect(html).toContain('&lt;img')
    expect(html).not.toContain('<img src=x')
  })
})
