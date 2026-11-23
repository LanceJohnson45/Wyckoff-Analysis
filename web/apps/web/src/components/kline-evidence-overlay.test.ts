import { describe, expect, it, vi } from 'vitest'
import type { IChartApi, ISeriesApi } from 'lightweight-charts'
import { attachEvidenceLines } from './kline-evidence-overlay'

const detach = vi.hoisted(() => vi.fn())
const markerCalls = vi.hoisted(() => vi.fn(() => ({ detach })))
vi.mock('lightweight-charts', () => ({ LineSeries: {}, createSeriesMarkers: markerCalls }))

describe('显式证据曲线的图表接线', () => {
  it('传入原日期/数值，确认日标记，并在切换时清理旧线与标记', () => {
    const series = { setData: vi.fn() } as unknown as ISeriesApi<'Line'>
    const chart = { addSeries: vi.fn(() => series), removeSeries: vi.fn() } as unknown as IChartApi
    const cleanup = attachEvidenceLines(chart, [{ label: '日线估算', color: '#8b5cf6', confirmedDate: '2026-11-20', points: [{ date: '2026-11-20', value: 12.5 }] }])
    expect(series.setData).toHaveBeenCalledWith([{ time: '2026-11-20', value: 12.5 }])
    expect(markerCalls).toHaveBeenCalledWith(series, [expect.objectContaining({ time: '2026-11-20', text: '确认' })])
    cleanup()
    expect(detach).toHaveBeenCalledTimes(1)
    expect(chart.removeSeries).toHaveBeenCalledWith(series)
    const count = markerCalls.mock.calls.length
    attachEvidenceLines(chart, [{ label: '截断窗口', color: '#8b5cf6', confirmedDate: '2026-11-19', points: [{ date: '2026-11-20', value: 12.5 }] }])()
    expect(markerCalls.mock.calls.length).toBe(count)
    const calls = vi.mocked(chart.addSeries).mock.calls.length
    attachEvidenceLines(chart, [{ label: '零量', color: '#8b5cf6', confirmedDate: '2026-11-20', points: [] }])()
    expect(chart.addSeries).toHaveBeenCalledTimes(calls)
  })
})
