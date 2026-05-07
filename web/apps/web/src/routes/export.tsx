import { useState } from 'react'
import { Download, Loader2 } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { useAuthStore } from '@/stores/auth'
import { usePreferences } from '@/lib/preferences'

export function ExportPage() {
  const user = useAuthStore((s) => s.user)
  const { t } = usePreferences()
  const [symbol, setSymbol] = useState('')
  const [days, setDays] = useState(320)
  const [adjust, setAdjust] = useState('qfq')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [preview, setPreview] = useState<Record<string, string | number>[] | null>(null)
  const [csvBlob, setCsvBlob] = useState<Blob | null>(null)
  const [fileName, setFileName] = useState('')

  async function getTickFlowKey(): Promise<string | null> {
    if (!user) return null
    const { data } = await supabase
      .from('user_settings')
      .select('tickflow_api_key')
      .eq('user_id', user.id)
      .single()
    return data?.tickflow_api_key || null
  }

  async function handleExport() {
    const code = symbol.trim().replace(/\D/g, '')
    if (code.length !== 6) {
      setError(t('common.invalidStockCode'))
      return
    }

    setError('')
    setLoading(true)
    setPreview(null)
    setCsvBlob(null)

    try {
      const apiKey = await getTickFlowKey()
      if (!apiKey) {
        setError(t('export.configureTickflow'))
        setLoading(false)
        return
      }

      const endDate = new Date()
      endDate.setDate(endDate.getDate() - 1)
      const end = formatDate(endDate)

      const startDate = new Date()
      startDate.setDate(startDate.getDate() - Math.ceil(days * 1.6))
      const start = formatDate(startDate)

      const url = `https://api.tickflow.io/v1/stock/history?symbol=${code}&start_date=${start}&end_date=${end}&adjust=${adjust}&limit=${days}`

      const resp = await fetch(url, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
      })

      if (!resp.ok) {
        const errBody = await resp.text()
        throw new Error(t('export.apiError', { status: resp.status, message: errBody.slice(0, 200) }))
      }

      const json = await resp.json()
      const rows: Record<string, string | number>[] = json.data || json.records || json || []

      if (!Array.isArray(rows) || rows.length === 0) {
        throw new Error(t('export.noData'))
      }

      setPreview(rows.slice(0, 10))

      const csvContent = arrayToCSV(rows)
      const blob = new Blob(['﻿' + csvContent], { type: 'text/csv;charset=utf-8;' })
      setCsvBlob(blob)
      setFileName(`${code}_ohlcv_${end}.csv`)
    } catch (err) {
      setError(err instanceof Error ? err.message : t('export.failed'))
    } finally {
      setLoading(false)
    }
  }

  function handleDownload() {
    if (!csvBlob || !fileName) return
    const url = URL.createObjectURL(csvBlob)
    const a = document.createElement('a')
    a.href = url
    a.download = fileName
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="flex h-full flex-col p-6">
      <h1 className="mb-6 text-xl font-semibold">{t('export.title')}</h1>

      {/* Form */}
      <div className="mb-6 flex flex-wrap items-end gap-3">
        <div>
          <label className="mb-1.5 block text-sm font-medium">{t('common.stockCode')}</label>
          <input
            type="text"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            placeholder={t('common.exampleCode')}
            maxLength={6}
            className="w-40 rounded-lg border border-border px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring/20"
            onKeyDown={(e) => e.key === 'Enter' && handleExport()}
          />
        </div>
        <div>
          <label className="mb-1.5 block text-sm font-medium">{t('export.days')}</label>
          <input
            type="number"
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            min={10}
            max={700}
            className="w-24 rounded-lg border border-border px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-ring/20"
          />
        </div>
        <div>
          <label className="mb-1.5 block text-sm font-medium">{t('export.adjust')}</label>
          <select
            value={adjust}
            onChange={(e) => setAdjust(e.target.value)}
            className="rounded-lg border border-border px-3 py-2 text-sm"
          >
            <option value="qfq">{t('export.qfq')}</option>
            <option value="hfq">{t('export.hfq')}</option>
            <option value="">{t('export.noneAdjust')}</option>
          </select>
        </div>
        <button
          onClick={handleExport}
          disabled={loading || !symbol.trim()}
          className="flex items-center gap-2 rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground disabled:opacity-50"
        >
          {loading ? <Loader2 size={16} className="animate-spin" /> : <Download size={16} />}
          {loading ? t('export.fetching') : t('export.fetch')}
        </button>
      </div>

      {error && (
        <div className="mb-4 rounded-lg bg-red-50 px-4 py-2.5 text-sm text-red-700 dark:bg-red-500/10 dark:text-red-200">
          {error}
          <a
            href="https://wyckoff-analysis-youngcanphoenix.streamlit.app/"
            target="_blank"
            rel="noopener noreferrer"
            className="ml-2 text-primary hover:underline"
          >
            {t('export.streamlitLink')}
          </a>
        </div>
      )}

      {/* Download button */}
      {csvBlob && (
        <div className="mb-4 flex items-center gap-3">
          <button
            onClick={handleDownload}
            className="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
          >
            <Download size={16} />
            {t('export.downloadCsv', { fileName })}
          </button>
          <span className="text-xs text-muted-foreground">
            {preview ? (preview.length >= 10 ? t('export.previewCountMore') : t('export.previewCount', { count: preview.length })) : ''}
          </span>
        </div>
      )}

      {/* Preview table */}
      {preview && preview.length > 0 && (
        <div className="min-h-0 flex-1 overflow-hidden rounded-lg border border-border">
          <div className="h-full overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-muted/80 backdrop-blur">
                <tr>
                  {Object.keys(preview[0]!).map((key) => (
                    <th key={key} className="whitespace-nowrap px-3 py-2 text-left font-medium">
                      {key}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.map((row, i) => (
                  <tr key={i} className="border-t border-border hover:bg-muted/20">
                    {Object.values(row).map((val, j) => (
                      <td key={j} className="whitespace-nowrap px-3 py-2">
                        {typeof val === 'number' ? val.toFixed?.(2) ?? val : String(val ?? '')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!preview && !loading && (
        <div className="flex flex-1 items-center justify-center text-muted-foreground">
          <div className="text-center">
            <div className="mb-3 text-4xl">📁</div>
            <p className="text-sm">{t('export.emptyTitle')}</p>
            <p className="mt-1 text-xs">{t('export.emptySubtitle')}</p>
            <a
              href="https://wyckoff-analysis-youngcanphoenix.streamlit.app/"
              target="_blank"
              rel="noopener noreferrer"
              className="mt-4 inline-block text-xs text-primary hover:underline"
            >
              {t('export.needBatch')}
            </a>
          </div>
        </div>
      )}
    </div>
  )
}

function formatDate(d: Date): string {
  return d.toISOString().slice(0, 10).replace(/-/g, '')
}

function arrayToCSV(rows: Record<string, unknown>[]): string {
  if (rows.length === 0 || !rows[0]) return ''
  const headers = Object.keys(rows[0]!)
  const lines = [headers.join(',')]
  for (const row of rows) {
    lines.push(headers.map((h) => {
      const v = row[h]
      if (v == null) return ''
      if (typeof v === 'string' && (v.includes(',') || v.includes('"'))) {
        return `"${v.replace(/"/g, '""')}"`
      }
      return String(v)
    }).join(','))
  }
  return lines.join('\n')
}
