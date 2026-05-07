import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { supabase } from '@/lib/supabase'
import { useAuthStore } from '@/stores/auth'
import { WyckoffLoading } from '@/components/loading'
import { usePreferences } from '@/lib/preferences'

interface Position {
  code: string
  name: string
  shares: number
  cost_price: number
  buy_dt: string | null
}

interface Portfolio {
  free_cash: number
  positions: Position[]
}

async function fetchPortfolio(userId: string): Promise<Portfolio> {
  const portfolioId = `USER_LIVE:${userId}`
  const [{ data: pf }, { data: positions }] = await Promise.all([
    supabase.from('portfolios').select('free_cash').eq('portfolio_id', portfolioId).single(),
    supabase.from('portfolio_positions').select('code, name, shares, cost_price, buy_dt').eq('portfolio_id', portfolioId).order('buy_dt', { ascending: false }),
  ])
  return { free_cash: pf?.free_cash || 0, positions: positions || [] }
}

export function PortfolioPage() {
  const user = useAuthStore((s) => s.user)
  const queryClient = useQueryClient()
  const { t } = usePreferences()
  const [editingCash, setEditingCash] = useState(false)
  const [cashInput, setCashInput] = useState('')

  const { data: portfolio, isLoading } = useQuery({
    queryKey: ['portfolio', user?.id],
    queryFn: () => fetchPortfolio(user!.id),
    enabled: !!user,
  })

  async function saveCash() {
    if (!user) return
    const portfolioId = `USER_LIVE:${user.id}`
    const val = parseFloat(cashInput)
    if (isNaN(val)) return

    await supabase
      .from('portfolios')
      .upsert({ portfolio_id: portfolioId, free_cash: val })

    setEditingCash(false)
    queryClient.invalidateQueries({ queryKey: ['portfolio', user.id] })
  }

  async function deletePosition(code: string) {
    if (!user) return
    const portfolioId = `USER_LIVE:${user.id}`
    await supabase
      .from('portfolio_positions')
      .delete()
      .eq('portfolio_id', portfolioId)
      .eq('code', code)
    queryClient.invalidateQueries({ queryKey: ['portfolio', user.id] })
  }

  if (isLoading) {
    return <WyckoffLoading />
  }

  const totalCost = portfolio?.positions.reduce((s, p) => s + p.shares * p.cost_price, 0) || 0
  const totalAssets = totalCost + (portfolio?.free_cash || 0)

  return (
    <div className="h-full p-6">
      <h1 className="mb-6 text-xl font-semibold">{t('portfolio.title')}</h1>

      {/* Summary Cards */}
      <div className="mb-6 grid grid-cols-3 gap-4">
        <SummaryCard label={t('portfolio.totalAssets')} value={`¥${totalAssets.toLocaleString()}`} />
        <SummaryCard
          label={t('portfolio.freeCash')}
          value={`¥${(portfolio?.free_cash || 0).toLocaleString()}`}
          onClick={() => {
            setEditingCash(true)
            setCashInput(String(portfolio?.free_cash || 0))
          }}
        />
        <SummaryCard label={t('portfolio.positionCount')} value={String(portfolio?.positions.length || 0)} />
      </div>

      {/* Cash Edit Modal */}
      {editingCash && (
        <div className="mb-4 flex items-center gap-2 rounded-lg border border-border p-3">
          <input
            type="number"
            value={cashInput}
            onChange={(e) => setCashInput(e.target.value)}
            className="flex-1 rounded-lg border border-border px-3 py-1.5 text-sm outline-none"
            autoFocus
          />
          <button onClick={saveCash} className="rounded-lg bg-primary px-3 py-1.5 text-sm text-primary-foreground">{t('action.save')}</button>
          <button onClick={() => setEditingCash(false)} className="rounded-lg border border-border px-3 py-1.5 text-sm">{t('action.cancel')}</button>
        </div>
      )}

      {/* Positions Table */}
      {portfolio?.positions.length === 0 ? (
        <div className="rounded-lg border border-border p-8 text-center text-sm text-muted-foreground">
          {t('portfolio.empty')}
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-4 py-2.5 text-left font-medium">{t('common.code')}</th>
                <th className="px-4 py-2.5 text-left font-medium">{t('common.name')}</th>
                <th className="px-4 py-2.5 text-right font-medium">{t('portfolio.shares')}</th>
                <th className="px-4 py-2.5 text-right font-medium">{t('portfolio.costPrice')}</th>
                <th className="px-4 py-2.5 text-right font-medium">{t('portfolio.buyDate')}</th>
                <th className="px-4 py-2.5 text-right font-medium">{t('portfolio.action')}</th>
              </tr>
            </thead>
            <tbody>
              {portfolio?.positions.map((p) => (
                <tr key={p.code} className="border-t border-border">
                  <td className="px-4 py-2.5 font-mono">{p.code}</td>
                  <td className="px-4 py-2.5">{p.name}</td>
                  <td className="px-4 py-2.5 text-right">{p.shares}</td>
                  <td className="px-4 py-2.5 text-right">{p.cost_price.toFixed(2)}</td>
                  <td className="px-4 py-2.5 text-right text-muted-foreground">{p.buy_dt || '-'}</td>
                  <td className="px-4 py-2.5 text-right">
                    <button
                      onClick={() => deletePosition(p.code)}
                      className="text-xs text-destructive hover:underline"
                    >
                      {t('action.delete')}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function SummaryCard({ label, value, onClick }: { label: string; value: string; onClick?: () => void }) {
  return (
    <div
      onClick={onClick}
      className={`rounded-lg border border-border p-4 ${onClick ? 'cursor-pointer hover:bg-muted/30' : ''}`}
    >
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-lg font-semibold">{value}</div>
    </div>
  )
}
