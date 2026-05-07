import { useQuery } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'
import { WyckoffLoading } from '@/components/loading'
import { usePreferences } from '@/lib/preferences'

interface TailBuyRecord {
  code: string
  name: string
  run_date: string
  signal_type: string
  rule_score: number
  priority_score: number
  llm_decision: string
  llm_reason: string
}

async function fetchTailBuy(): Promise<TailBuyRecord[]> {
  const { data } = await supabase
    .from('tail_buy_history')
    .select('code, name, run_date, signal_type, rule_score, priority_score, llm_decision, llm_reason')
    .order('run_date', { ascending: false })
    .limit(200)
  return data || []
}

export function TailBuyPage() {
  const { t } = usePreferences()
  const { data = [], isLoading } = useQuery({
    queryKey: ['tail-buy'],
    queryFn: fetchTailBuy,
  })

  if (isLoading) {
    return <WyckoffLoading />
  }

  return (
    <div className="flex h-full flex-col p-6">
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-xl font-semibold">{t('tailBuy.title')}</h1>
        <span className="text-xs text-muted-foreground">{t('tailBuy.total', { count: data.length })}</span>
      </div>

      {data.length === 0 ? (
        <div className="flex flex-1 items-center justify-center text-muted-foreground">
          <div className="text-center">
            <div className="mb-3 text-4xl">🌙</div>
            <p className="text-sm">{t('tailBuy.empty')}</p>
            <p className="mt-1 text-xs">{t('tailBuy.emptySubtitle')}</p>
          </div>
        </div>
      ) : (
        <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border">
          <div className="h-full overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                <tr>
                  <th className="px-3 py-2.5 text-left font-medium">{t('common.code')}</th>
                  <th className="px-3 py-2.5 text-left font-medium">{t('common.name')}</th>
                  <th className="px-3 py-2.5 text-right font-medium">{t('common.date')}</th>
                  <th className="px-3 py-2.5 text-center font-medium">{t('tailBuy.signal')}</th>
                  <th className="px-3 py-2.5 text-right font-medium">{t('tailBuy.ruleScore')}</th>
                  <th className="px-3 py-2.5 text-right font-medium">{t('tailBuy.priorityScore')}</th>
                  <th className="px-3 py-2.5 text-center font-medium">{t('tailBuy.llmDecision')}</th>
                  <th className="px-3 py-2.5 text-left font-medium">{t('tailBuy.reason')}</th>
                </tr>
              </thead>
              <tbody>
                {data.map((r, i) => (
                  <tr key={`${r.code}-${r.run_date}-${i}`} className="border-t border-border hover:bg-muted/20">
                    <td className="px-3 py-2 font-mono">{String(r.code).padStart(6, '0')}</td>
                    <td className="px-3 py-2">{r.name}</td>
                    <td className="px-3 py-2 text-right text-muted-foreground">{r.run_date}</td>
                    <td className="px-3 py-2 text-center">
                      <span className="inline-flex rounded-full bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700 dark:bg-blue-500/10 dark:text-blue-200">
                        {r.signal_type || '-'}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right">{r.rule_score?.toFixed(1)}</td>
                    <td className="px-3 py-2 text-right">{r.priority_score?.toFixed(1)}</td>
                    <td className="px-3 py-2 text-center">
                      <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                        r.llm_decision === 'BUY'
                          ? 'bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-200'
                          : 'bg-muted text-muted-foreground'
                      }`}>
                        {r.llm_decision || '-'}
                      </span>
                    </td>
                    <td className="max-w-[200px] truncate px-3 py-2 text-xs text-muted-foreground" title={r.llm_reason}>
                      {r.llm_reason || '-'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
