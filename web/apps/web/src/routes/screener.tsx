import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Filter, RefreshCw } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { usePreferences } from '@/lib/preferences'

interface ScreenerRow {
  code: number
  name: string
  recommend_date: number
  funnel_score: number | null
  change_pct: number | null
  initial_price: number | null
  current_price: number | null
}

async function fetchDates(): Promise<number[]> {
  const { data } = await supabase
    .from('recommendation_tracking')
    .select('recommend_date')
    .eq('is_ai_recommended', true)
    .order('recommend_date', { ascending: false })
    .limit(200)
  if (!data || data.length === 0) return []
  return [...new Set(data.map(r => r.recommend_date))].sort((a, b) => b - a)
}

async function fetchRows(date: number): Promise<ScreenerRow[]> {
  const { data } = await supabase
    .from('recommendation_tracking')
    .select('code, name, recommend_date, funnel_score, change_pct, initial_price, current_price')
    .eq('is_ai_recommended', true)
    .eq('recommend_date', date)
    .order('funnel_score', { ascending: false })
  return data || []
}

export function ScreenerPage() {
  const [selectedDate, setSelectedDate] = useState<number | null>(null)
  const { t } = usePreferences()

  const { data: allDates = [] } = useQuery({
    queryKey: ['screener-dates'],
    queryFn: fetchDates,
  })

  const activeDate = selectedDate ?? allDates[0] ?? null
  const latestDate = allDates[0] ?? null

  const { data: rows = [], isLoading: loading, refetch } = useQuery({
    queryKey: ['screener-rows', activeDate],
    queryFn: () => fetchRows(activeDate!),
    enabled: !!activeDate,
  })

  function handleRefresh() {
    refetch()
  }

  const fmtDate = (d: number) => {
    const s = String(d)
    return s.length === 8 ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}` : String(d)
  }

  return (
    <div className="flex h-full flex-col p-6">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">{t('screener.title')}</h1>
          <Filter size={18} className="text-muted-foreground" />
        </div>
        <button
          onClick={handleRefresh}
          disabled={loading}
          className="flex items-center gap-1.5 rounded-lg border border-border px-3 py-1.5 text-sm text-muted-foreground hover:bg-muted/50 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          {t('action.refresh')}
        </button>
      </div>

      {/* Date selector */}
      {allDates.length > 0 && (
        <div className="mb-4 flex flex-wrap items-center gap-2">
          <span className="text-xs text-muted-foreground">{t('screener.date')}</span>
          {allDates.slice(0, 10).map((d) => (
            <button
              key={d}
              onClick={() => setSelectedDate(d)}
              className={`rounded-full px-3 py-1 text-xs transition-colors ${
                activeDate === d
                  ? 'bg-primary text-primary-foreground'
                  : 'border border-border text-muted-foreground hover:bg-muted/50'
              }`}
            >
              {fmtDate(d)}
            </button>
          ))}
        </div>
      )}

      {/* Summary */}
      {!loading && rows.length > 0 && (
        <div className="mb-4 flex items-center gap-6">
          <div className="rounded-lg bg-primary/5 px-4 py-2">
            <div className="text-2xl font-bold text-primary">{rows.length}</div>
            <div className="text-xs text-muted-foreground">{t('screener.aiCandidates')}</div>
          </div>
          {latestDate && (
            <div className="text-xs text-muted-foreground">
              {t('screener.latestDate', { date: fmtDate(latestDate) })}
            </div>
          )}
        </div>
      )}

      {/* Table */}
      <div className="min-h-0 flex-1 overflow-auto rounded-lg border border-border">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-muted/30 text-xs text-muted-foreground">
            <tr>
              <th className="px-4 py-2.5 text-left font-medium">{t('common.code')}</th>
              <th className="px-4 py-2.5 text-left font-medium">{t('common.name')}</th>
              <th className="px-4 py-2.5 text-right font-medium">{t('screener.funnelScore')}</th>
              <th className="px-4 py-2.5 text-right font-medium">{t('screener.recommendPrice')}</th>
              <th className="px-4 py-2.5 text-right font-medium">{t('tracking.currentPrice')}</th>
              <th className="px-4 py-2.5 text-right font-medium">{t('tracking.changePct')}</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {loading ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                  {t('common.loading')}
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-muted-foreground">
                  {t('screener.empty')}
                </td>
              </tr>
            ) : (
              rows.map((r) => {
                const code = String(r.code).padStart(6, '0')
                const chg = r.change_pct
                return (
                  <tr key={r.code} className="hover:bg-muted/20">
                    <td className="px-4 py-2.5 font-mono text-xs">{code}</td>
                    <td className="px-4 py-2.5">{r.name}</td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {r.funnel_score != null ? r.funnel_score.toFixed(2) : '--'}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {r.initial_price?.toFixed(2) || '--'}
                    </td>
                    <td className="px-4 py-2.5 text-right font-mono">
                      {r.current_price?.toFixed(2) || '--'}
                    </td>
                    <td className={`px-4 py-2.5 text-right font-mono font-medium ${
                      chg == null ? '' : chg >= 0 ? 'text-up' : 'text-down'
                    }`}>
                      {chg != null ? `${chg >= 0 ? '+' : ''}${chg.toFixed(2)}%` : '--'}
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      <p className="mt-3 text-xs text-muted-foreground">
        {t('screener.source')}
      </p>
    </div>
  )
}
