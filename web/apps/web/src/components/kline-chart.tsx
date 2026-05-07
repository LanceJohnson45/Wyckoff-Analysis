import { useEffect, useMemo, useRef } from 'react'
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  LineStyle,
  createSeriesMarkers,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type LineData,
  type SeriesMarker,
  type Time,
} from 'lightweight-charts'

interface KlineData {
  date: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

interface KlineChartProps {
  data: KlineData[]
  height?: number
}

interface StructureSnapshot {
  changePct: number
  latestClose: number
  ma20: number
  ma50: number
  support: number
  resistance: number
  volumeRatio: number
  phase: string
  tone: 'strong' | 'watch' | 'weak'
}

export function KlineChart({ data, height = 400 }: KlineChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const structure = useMemo(() => buildStructureSnapshot(data), [data])

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return

    const theme = readChartTheme()
    const chart = createChart(containerRef.current, {
      height,
      layout: {
        background: { color: theme.background },
        textColor: theme.mutedText,
        fontSize: 11,
      },
      grid: {
        vertLines: { color: theme.grid },
        horzLines: { color: theme.grid },
      },
      crosshair: {
        mode: 0,
      },
      rightPriceScale: {
        borderColor: theme.border,
      },
      timeScale: {
        borderColor: theme.border,
        timeVisible: false,
      },
    })

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: theme.up,
      downColor: theme.down,
      borderUpColor: theme.up,
      borderDownColor: theme.down,
      wickUpColor: theme.up,
      wickDownColor: theme.down,
    })

    const ma5Series = chart.addSeries(LineSeries, {
      color: '#f59e0b',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const ma20Series = chart.addSeries(LineSeries, {
      color: '#2563eb',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    })
    const ma50Series = chart.addSeries(LineSeries, {
      color: '#7c3aed',
      lineWidth: 2,
      lineStyle: LineStyle.Dashed,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    })

    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.84, bottom: 0 },
    })

    const candles: CandlestickData<Time>[] = data.map((d) => ({
      time: d.date as Time,
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }))

    const volumes: HistogramData<Time>[] = data.map((d) => ({
      time: d.date as Time,
      value: d.volume,
      color: d.close >= d.open ? `${theme.up}4d` : `${theme.down}4d`,
    }))

    candleSeries.setData(candles)
    ma5Series.setData(movingAverage(data, 5))
    ma20Series.setData(movingAverage(data, 20))
    ma50Series.setData(movingAverage(data, 50))
    volumeSeries.setData(volumes)
    createSeriesMarkers(candleSeries, buildMarkers(data))

    const levels = buildPriceLevels(data)
    if (levels.support > 0) {
      candleSeries.createPriceLine({
        price: levels.support,
        color: theme.support,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: '支撑',
      })
    }
    if (levels.resistance > 0) {
      candleSeries.createPriceLine({
        price: levels.resistance,
        color: theme.resistance,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: '压力',
      })
    }

    chart.timeScale().fitContent()
    chartRef.current = chart

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth })
      }
    }
    window.addEventListener('resize', handleResize)
    handleResize()

    return () => {
      window.removeEventListener('resize', handleResize)
      chart.remove()
      chartRef.current = null
    }
  }, [data, height])

  return (
    <div className="space-y-3">
      {structure && (
        <div className="grid gap-2 text-xs sm:grid-cols-2 lg:grid-cols-4">
          <Metric label="最新收盘" value={`${formatPrice(structure.latestClose)} (${formatPct(structure.changePct)})`} tone={structure.tone} />
          <Metric label="结构状态" value={structure.phase} tone={structure.tone} />
          <Metric label="支撑 / 压力" value={`${formatPrice(structure.support)} / ${formatPrice(structure.resistance)}`} />
          <Metric label="量能 / 均线" value={`${structure.volumeRatio.toFixed(1)}x · MA20 ${formatPrice(structure.ma20)} · MA50 ${formatPrice(structure.ma50)}`} />
        </div>
      )}
      <div ref={containerRef} className="h-[350px] w-full overflow-hidden rounded-lg border border-border bg-background sm:h-auto" />
      <div className="flex flex-wrap gap-3 text-[11px] text-muted-foreground">
        <Legend color="#f59e0b" label="MA5" />
        <Legend color="#2563eb" label="MA20" />
        <Legend color="#7c3aed" label="MA50" />
        <Legend color="#ef4444" label="SOS/突破" />
        <Legend color="#2563eb" label="测试" />
        <Legend color="#10b981" label="供应风险" />
      </div>
    </div>
  )
}

function Metric({ label, value, tone = 'watch' }: { label: string; value: string; tone?: StructureSnapshot['tone'] }) {
  const toneClass = tone === 'strong'
    ? 'border-red-200 bg-red-50 text-red-700 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-200'
    : tone === 'weak'
      ? 'border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-500/30 dark:bg-emerald-500/10 dark:text-emerald-200'
      : 'border-border bg-muted/40 text-foreground'

  return (
    <div className={`rounded-lg border px-3 py-2 ${toneClass}`}>
      <div className="mb-1 text-[11px] text-muted-foreground">{label}</div>
      <div className="font-medium">{value}</div>
    </div>
  )
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className="h-2 w-2 rounded-full" style={{ backgroundColor: color }} />
      {label}
    </span>
  )
}

function readChartTheme() {
  const style = getComputedStyle(document.documentElement)
  const color = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback
  return {
    background: color('--color-background', '#ffffff'),
    mutedText: color('--color-muted-foreground', '#6b7194'),
    border: color('--color-border', '#e2e5f1'),
    grid: document.documentElement.classList.contains('dark') ? '#202938' : '#eef1f6',
    up: color('--color-up', '#ef4444'),
    down: color('--color-down', '#10b981'),
    support: '#2563eb',
    resistance: '#f59e0b',
  }
}

function movingAverage(data: KlineData[], period: number): LineData<Time>[] {
  if (data.length < period) return []
  const points: LineData<Time>[] = []
  for (let index = period - 1; index < data.length; index += 1) {
    const window = data.slice(index - period + 1, index + 1)
    points.push({ time: data[index]!.date as Time, value: avg(window.map((d) => d.close)) })
  }
  return points
}

function buildMarkers(data: KlineData[]): SeriesMarker<Time>[] {
  const markers: SeriesMarker<Time>[] = []
  const start = Math.max(20, data.length - 150)
  for (let index = start; index < data.length; index += 1) {
    const current = data[index]!
    const previous = data.slice(Math.max(0, index - 20), index)
    if (previous.length < 10) continue

    const volume20 = avg(previous.map((d) => d.volume))
    const previousHigh = Math.max(...previous.map((d) => d.high))
    const range = Math.max(current.high - current.low, 0.01)
    const closePosition = (current.close - current.low) / range
    const body = Math.abs(current.close - current.open)
    const upperShadow = current.high - Math.max(current.close, current.open)

    if (current.close > previousHigh && current.volume > volume20 * 1.35 && closePosition > 0.65) {
      markers.push({
        time: current.date as Time,
        position: 'belowBar',
        shape: 'arrowUp',
        color: '#ef4444',
        text: 'SOS',
        size: 1.2,
      })
    } else if (current.volume < volume20 * 0.55 && closePosition > 0.55) {
      markers.push({
        time: current.date as Time,
        position: 'belowBar',
        shape: 'circle',
        color: '#2563eb',
        text: '测试',
      })
    }

    if (upperShadow > Math.max(body * 1.8, range * 0.35) && closePosition < 0.45 && current.volume > volume20 * 1.15) {
      markers.push({
        time: current.date as Time,
        position: 'aboveBar',
        shape: 'arrowDown',
        color: '#10b981',
        text: '供应',
      })
    }
  }
  return markers.slice(-12)
}

function buildStructureSnapshot(data: KlineData[]): StructureSnapshot | null {
  if (data.length === 0) return null
  const latest = data[data.length - 1]!
  const previous = data[data.length - 2]
  const ma20 = data.length >= 20 ? avg(data.slice(-20).map((d) => d.close)) : latest.close
  const ma50 = data.length >= 50 ? avg(data.slice(-50).map((d) => d.close)) : ma20
  const levels = buildPriceLevels(data)
  const volumeBase = data.length >= 21 ? avg(data.slice(-21, -1).map((d) => d.volume)) : avg(data.map((d) => d.volume))
  const changePct = previous ? (latest.close / previous.close - 1) * 100 : 0
  const volumeRatio = volumeBase > 0 ? latest.volume / volumeBase : 0
  const upperBand = levels.support + (levels.resistance - levels.support) * 0.72
  const lowerBand = levels.support + (levels.resistance - levels.support) * 0.28

  if (latest.close > ma20 && ma20 >= ma50 && latest.close >= upperBand) {
    return { changePct, latestClose: latest.close, ma20, ma50, volumeRatio, ...levels, phase: '右侧走强', tone: 'strong' }
  }
  if (latest.close < ma20 || latest.close <= lowerBand) {
    return { changePct, latestClose: latest.close, ma20, ma50, volumeRatio, ...levels, phase: '回踩/弱势', tone: 'weak' }
  }
  return { changePct, latestClose: latest.close, ma20, ma50, volumeRatio, ...levels, phase: '区间观察', tone: 'watch' }
}

function buildPriceLevels(data: KlineData[]) {
  const recent = data.slice(-60)
  return {
    support: Math.min(...recent.map((d) => d.low)),
    resistance: Math.max(...recent.map((d) => d.high)),
  }
}

function avg(values: number[]): number {
  return values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : 0
}

function formatPrice(value: number): string {
  return Number.isFinite(value) ? value.toFixed(2) : '--'
}

function formatPct(value: number): string {
  if (!Number.isFinite(value)) return '--'
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`
}
