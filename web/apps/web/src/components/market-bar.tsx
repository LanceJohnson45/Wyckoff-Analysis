import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'
import { usePreferences, type TranslationKey } from '@/lib/preferences'

interface MarketSignal {
  benchmark_regime: string
  banner_title: string
  banner_message: string
  banner_tone: string
  main_index_close: number
  main_index_today_pct: number
  main_index_date: string
  a50_close: number
  a50_pct_chg: number
  a50_date: string
  vix_close: number
  vix_pct_chg: number
  vix_date: string
}

const REGIME_COLORS: Record<string, { className: string; labelKey: TranslationKey }> = {
  RISK_ON: { className: 'bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-200', labelKey: 'market.riskOn' },
  NEUTRAL: { className: 'bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-200', labelKey: 'market.neutral' },
  RISK_OFF: { className: 'bg-purple-50 text-purple-700 dark:bg-purple-500/10 dark:text-purple-200', labelKey: 'market.riskOff' },
  CRASH: { className: 'bg-orange-50 text-orange-700 dark:bg-orange-500/10 dark:text-orange-200', labelKey: 'market.crash' },
  BLACK_SWAN: { className: 'bg-red-100 text-red-800 dark:bg-red-500/20 dark:text-red-100', labelKey: 'market.blackSwan' },
}

const TONE_META: Record<string, { className: string; labelKey: TranslationKey }> = {
  '乐观': { className: 'bg-sky-50 text-sky-700 dark:bg-sky-500/10 dark:text-sky-200', labelKey: 'market.optimistic' },
  '谨慎乐观': { className: 'bg-cyan-50 text-cyan-700 dark:bg-cyan-500/10 dark:text-cyan-200', labelKey: 'market.cautiouslyOptimistic' },
  '谨慎': { className: 'bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-200', labelKey: 'market.cautious' },
  '保守': { className: 'bg-amber-50 text-amber-700 dark:bg-amber-500/10 dark:text-amber-200', labelKey: 'market.defensive' },
  '恶劣': { className: 'bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-200', labelKey: 'market.blackSwan' },
}

function mergeRows(data: Record<string, unknown>[]): { merged: Record<string, unknown>; mainDate: string; a50Date: string; vixDate: string } {
  const merged: Record<string, unknown> = {}
  let mainDate = '', a50Date = '', vixDate = ''
  for (const row of data) {
    for (const key of ['benchmark_regime', 'main_index_close', 'main_index_today_pct', 'main_index_ma50', 'main_index_ma200']) {
      if (merged[key] == null && row[key] != null) {
        merged[key] = row[key]
        if (key === 'main_index_close') mainDate = (row.trade_date as string) || ''
      }
    }
    for (const key of ['a50_close', 'a50_pct_chg']) {
      if (merged[key] == null && row[key] != null) {
        merged[key] = row[key]
        if (key === 'a50_close' && !a50Date) a50Date = (row.a50_value_date as string) || (row.trade_date as string) || ''
      }
    }
    for (const key of ['vix_close', 'vix_pct_chg']) {
      if (merged[key] == null && row[key] != null) {
        merged[key] = row[key]
        if (key === 'vix_close' && !vixDate) vixDate = (row.vix_value_date as string) || (row.trade_date as string) || ''
      }
    }
    for (const key of ['banner_title', 'banner_message', 'banner_tone']) {
      if (!merged[key] && row[key]) merged[key] = row[key]
    }
  }
  return { merged, mainDate, a50Date, vixDate }
}

async function fetchSignal(): Promise<MarketSignal | null> {
  const { data } = await supabase
    .from('market_signal_daily')
    .select('*')
    .order('trade_date', { ascending: false })
    .limit(5)

  if (!data || data.length === 0) return null

  const { merged, mainDate, a50Date, vixDate } = mergeRows(data)

  return {
    benchmark_regime: String(merged.benchmark_regime || 'NEUTRAL'),
    banner_title: String(merged.banner_title || ''),
    banner_message: String(merged.banner_message || ''),
    banner_tone: String(merged.banner_tone || '谨慎'),
    main_index_close: Number(merged.main_index_close || 0),
    main_index_today_pct: Number(merged.main_index_today_pct || 0),
    main_index_date: mainDate,
    a50_close: Number(merged.a50_close || 0),
    a50_pct_chg: Number(merged.a50_pct_chg || 0),
    a50_date: a50Date,
    vix_close: Number(merged.vix_close || 0),
    vix_pct_chg: Number(merged.vix_pct_chg || 0),
    vix_date: vixDate,
  }
}

export function MarketBar() {
  const { t } = usePreferences()
  const { data: signal } = useQuery({
    queryKey: ['market-signal'],
    queryFn: fetchSignal,
    refetchInterval: 60_000,
  })

  if (!signal) return null

  const regime = REGIME_COLORS[signal.benchmark_regime] || REGIME_COLORS.NEUTRAL!
  const tone = TONE_META[signal.banner_tone] || TONE_META['谨慎']!

  const fmtPct = (v: number) => v ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '--'
  const fmtDate = (d: string) => d ? d.slice(5).replace('-', '/') : ''

  return (
    <div className="border-b border-border bg-background px-6 py-2.5">
      <div className="flex flex-wrap items-center gap-4">
        <span className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold ${tone.className}`}>
          {t(tone.labelKey)}
        </span>

        <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[11px] ${regime.className}`}>
          {t(regime.labelKey)}
        </span>

        {signal.main_index_close > 0 && (
          <div className="flex items-center gap-1.5">
            <span className="text-xs text-muted-foreground">{t('market.mainIndex')}</span>
            <span className="text-sm font-medium">{signal.main_index_close.toFixed(0)}</span>
            <span className={`text-xs font-medium ${signal.main_index_today_pct >= 0 ? 'text-up' : 'text-down'}`}>
              {fmtPct(signal.main_index_today_pct)}
            </span>
            {signal.main_index_date && <span className="text-[10px] text-muted-foreground">{fmtDate(signal.main_index_date)}</span>}
          </div>
        )}

        {signal.a50_close > 0 && (
          <div className="flex items-center gap-1.5">
            <span className="text-xs text-muted-foreground">A50</span>
            <span className="text-xs font-medium">{signal.a50_close.toFixed(0)}</span>
            <span className={`text-xs font-medium ${signal.a50_pct_chg >= 0 ? 'text-up' : 'text-down'}`}>
              {fmtPct(signal.a50_pct_chg)}
            </span>
            {signal.a50_date && <span className="text-[10px] text-muted-foreground">{fmtDate(signal.a50_date)}</span>}
          </div>
        )}

        {signal.vix_close > 0 && (
          <div className="flex items-center gap-1.5">
            <span className="text-xs text-muted-foreground">VIX</span>
            <span className="text-xs font-medium">{signal.vix_close.toFixed(1)}</span>
            <span className={`text-xs font-medium ${signal.vix_pct_chg <= 0 ? 'text-up' : 'text-down'}`}>
              {fmtPct(signal.vix_pct_chg)}
            </span>
            {signal.vix_date && <span className="text-[10px] text-muted-foreground">{fmtDate(signal.vix_date)}</span>}
          </div>
        )}

        {signal.banner_title && (
          <span className="ml-auto text-xs font-medium text-foreground">{signal.banner_title}</span>
        )}
      </div>

      {signal.banner_message && (
        <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{signal.banner_message}</p>
      )}
    </div>
  )
}
