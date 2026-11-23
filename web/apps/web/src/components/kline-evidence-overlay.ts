import { useEffect, type MutableRefObject } from 'react'
import { LineSeries, createSeriesMarkers, type IChartApi, type ISeriesApi, type ISeriesMarkersPluginApi, type Time } from 'lightweight-charts'

export interface EvidenceChartLine {
  label: string
  color: string
  points: { date: string; value: number }[]
  confirmedDate: string
}
const EMPTY_LINES: EvidenceChartLine[] = []

export function attachEvidenceLines(chart: IChartApi, lines: EvidenceChartLine[]): () => void {
  const added: ISeriesApi<'Line'>[] = []
  const markers: ISeriesMarkersPluginApi<Time>[] = []
  for (const line of lines) {
    if (line.points.length === 0) continue
    const series = chart.addSeries(LineSeries, { title: line.label, color: line.color, lineWidth: 2, priceLineVisible: false, lastValueVisible: false })
    added.push(series)
    series.setData(line.points.map(point => ({ time: point.date as Time, value: point.value })))
    if (line.points.some(point => point.date === line.confirmedDate)) markers.push(createSeriesMarkers(series, [{ time: line.confirmedDate as Time, position: 'aboveBar', shape: 'circle', color: line.color, text: '确认' }]))
  }
  return () => { markers.forEach(marker => marker.detach()); added.forEach(series => chart.removeSeries(series)) }
}

export function useEvidenceLines(refs: MutableRefObject<{ chart: IChartApi } | null>, lines: EvidenceChartLine[] | undefined, height: number) {
  useEffect(() => {
    const current = refs.current
    if (!current) return
    const detach = attachEvidenceLines(current.chart, lines ?? EMPTY_LINES)
    // 主图在尺寸变化或卸载时先销毁，不能再次操作已经销毁的图表。
    return () => { if (refs.current === current) detach() }
  }, [refs, lines, height])
}
