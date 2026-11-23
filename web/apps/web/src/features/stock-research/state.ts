import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery, useQueryClient, type QueryKey } from '@tanstack/react-query'
import { createResearchRun, controlResearchRun, decodeResearchData, readLatestReport, readReport, readRuns, readRevisions, RESEARCH_BASE, reportSchema, type ResearchReport, type RunStatus } from './api'

export const isRunning = (status: RunStatus) => status === 'pending' || status === 'running'
export const strategyLabels = { martin: 'Martin 趋势回调', wyckoff: 'Wyckoff 量价结构' }
export const statusLabels: Record<RunStatus, string> = { pending: '已受理', running: '进行中', completed: '已完成', partial: '部分完成', failed: '未完成', cancelled: '已取消' }

export function newestReport(previous: unknown, next: unknown): unknown {
  const before = reportSchema.safeParse(previous)
  const after = reportSchema.safeParse(next)
  return before.success && after.success && before.data.run.run_id === after.data.run.run_id && before.data.revision > after.data.revision ? previous : next
}

function useReportEvents(runId: string | null, active: boolean, key: QueryKey) {
  const client = useQueryClient()
  const [problem, setProblem] = useState('')
  useEffect(() => {
    if (!runId || !active) return
    const cached = client.getQueryData<ResearchReport | null>(key)
    const source = new EventSource(`${RESEARCH_BASE}/v1/research-runs/${runId}/events?after=${cached?.revision ?? 0}`)
    const onReport = (event: Event) => {
      try {
        if (!(event instanceof MessageEvent) || typeof event.data !== 'string') throw new Error('invalid event')
        const next = decodeResearchData(JSON.parse(event.data), reportSchema)
        if (next.run.run_id !== runId) throw new Error('foreign run')
        client.setQueryData(key, (previous: unknown) => newestReport(previous, next))
        void client.invalidateQueries({ queryKey: ['research', 'revisions', runId] })
        void client.invalidateQueries({ queryKey: ['research', 'runs'] })
        setProblem('')
      } catch { source.close(); setProblem('研究进度响应异常，请刷新报告') }
    }
    source.addEventListener('report', onReport)
    source.onopen = () => setProblem('')
    source.onerror = () => setProblem('进度连接中断，正在重新连接；已保存的报告仍可查看')
    return () => { source.removeEventListener('report', onReport); source.close() }
  }, [active, client, key, runId])
  return active ? problem : ''
}

export function newYorkDay(date = new Date()): string {
  const parts = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(date)
  const read = (type: string) => { const part = parts.find((item) => item.type === type); if (!part) throw new Error('无法确定纽约交易日期'); return part.value }
  return `${read('year')}-${read('month')}-${read('day')}`
}

export function useStockResearch() {
  const client = useQueryClient()
  const [selection, setSelection] = useState<{ runId: string | null; reportId: string | null }>({ runId: null, reportId: null })
  const [page, setPage] = useState(1)
  const key = useMemo(() => ['research', 'report', selection.reportId || selection.runId || 'latest'], [selection])
  const report = useQuery({ queryKey: key, queryFn: ({ signal }) => selection.reportId ? readReport(selection.reportId, signal) : readLatestReport(signal, selection.runId || undefined), structuralSharing: newestReport, retry: false, refetchOnWindowFocus: false })
  const runId = selection.runId || report.data?.run.run_id || null
  const active = !selection.reportId && !report.isError && (report.data ? isRunning(report.data.run.status) : runId !== null)
  const connectionProblem = useReportEvents(runId, active, key)
  const runs = useQuery({ queryKey: ['research', 'runs', page], queryFn: ({ signal }) => readRuns(page, signal), retry: false, refetchOnWindowFocus: false })
  const revisions = useQuery({ queryKey: ['research', 'revisions', runId], queryFn: ({ signal }) => { if (!runId) throw new Error('尚未选择研究运行'); return readRevisions(runId, signal) }, enabled: runId !== null, retry: false, refetchOnWindowFocus: false })
  const actions = useResearchActions(report.data || null, (id: string) => setSelection({ runId: id, reportId: null }))
  return { report, runs, revisions, active, connectionProblem, page, setPage, runId, fixed: selection.reportId !== null, actions,
    selectRun: (id: string | null) => setSelection({ runId: id, reportId: null }),
    selectRevision: (id: string) => setSelection({ runId, reportId: id }),
    refresh: () => { void report.refetch(); void runs.refetch(); if (runId) void revisions.refetch(); void client.invalidateQueries({ queryKey: ['research', 'revisions', runId] }) },
  }
}

function useResearchActions(report: ResearchReport | null, selectRun: (id: string) => void) {
  const client = useQueryClient()
  const [target, setTarget] = useState(newYorkDay)
  const [busy, setBusy] = useState<'create' | 'retry' | 'cancel' | null>(null)
  const [error, setError] = useState('')
  const request = useRef<{ body: string; key: string } | null>(null)
  const inFlight = useRef(false)
  const act = async (kind: 'create' | 'retry' | 'cancel', force = false) => {
    if (inFlight.current) return
    inFlight.current = true
    setBusy(kind); setError('')
    try {
      let accepted
      if (kind === 'create') {
        if (force && !report) throw new Error('请先选择原研究报告')
        const body = { market: 'us' as const, target_session: force && report ? report.run.target_session : target, preview: true as const, ...(force && report ? { force: true, parent_run_id: report.run.run_id } : {}) }
        const canonical = JSON.stringify(body)
        if (!request.current || request.current.body !== canonical) request.current = { body: canonical, key: crypto.randomUUID() }
        accepted = await createResearchRun(body, request.current.key)
      } else {
        if (!report) throw new Error('请先选择研究报告')
        accepted = await controlResearchRun(report.run.run_id, kind)
      }
      selectRun(accepted.run_id)
      await client.invalidateQueries({ queryKey: ['research'] })
    } catch (reason) { setError(reason instanceof Error ? reason.message : '研究操作未确认，请刷新运行记录') }
    finally { inFlight.current = false; setBusy(null) }
  }
  return { target, setTarget, busy, error, act }
}
