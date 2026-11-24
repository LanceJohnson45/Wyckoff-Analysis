import { useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { controlEvaluation, createEvaluation, readCurrentParameters, readEvaluation, readEvaluations, readEvaluationResult, ResearchApiError } from './api'
import { evaluationActive, type CreateEvaluation, type EvaluationJob } from './evaluation-contract'

type Action = { kind: 'create'; body: CreateEvaluation } | { kind: 'confirm' } | { kind: 'cancel' | 'retry' | 'read'; id: string }
type Intent = { body: CreateEvaluation; key: string }

async function executeAction(action: Action, intent: Intent | null): Promise<EvaluationJob> {
  if (action.kind === 'create' || action.kind === 'confirm') {
    if (!intent) throw new Error('原提交尚未固定，请重新读取评估记录')
    return createEvaluation(intent.body, intent.key)
  }
  return action.kind === 'read' ? readEvaluation(action.id) : controlEvaluation(action.id, action.kind)
}

function useEvaluationActions(accept: (job: EvaluationJob) => void) {
  const guard = useRef(false), original = useRef<Intent | null>(null)
  const [busy, setBusy] = useState<Action['kind'] | null>(null)
  const [pending, setPending] = useState<CreateEvaluation | null>(null)
  const [uncertain, setUncertain] = useState(false)
  const [readbackId, setReadbackId] = useState<string | null>(null)
  const [message, setMessage] = useState('')
  async function act(action: Action) {
    if (guard.current || original.current && action.kind !== 'confirm' || readbackId && (action.kind !== 'read' || action.id !== readbackId)) return
    if (action.kind === 'confirm' && !original.current) return
    if (action.kind === 'create') {
      original.current = { body: { ...action.body }, key: crypto.randomUUID() }
      setPending(original.current.body)
    }
    guard.current = true; setBusy(action.kind); setMessage('')
    const submission = action.kind === 'create' || action.kind === 'confirm'
    try {
      const job = await executeAction(action, original.current)
      accept(job)
      if (submission) { original.current = null; setPending(null); setUncertain(false) }
      setReadbackId(null)
      setMessage(submission ? '评估已受理，任务与原输入会分别留档。' : action.kind === 'read' ? '已读取原任务状态。' : action.kind === 'cancel' ? '已核对取消请求；实际状态如下。' : '已受理原任务续跑，期限与输入保持。')
    } catch (error) {
      const unknown = error instanceof ResearchApiError && (error.status === 0 || error.status >= 500 || error.code === 'invalid_response')
      if (submission) {
        setUncertain(unknown)
        if (!unknown) { original.current = null; setPending(null) }
      } else if (unknown && 'id' in action) setReadbackId(action.id)
      setMessage(error instanceof Error ? error.message : '评估操作尚未确认')
    } finally { guard.current = false; setBusy(null) }
  }
  return { act, busy, pending, uncertain, readbackId, message, blocked: busy !== null || uncertain || readbackId !== null }
}

export function useEvaluationWorkspace(active: boolean) {
  const client = useQueryClient()
  const [page, setPage] = useState(1), [selected, setSelected] = useState<string | null>(null)
  const parameters = useQuery({ queryKey: ['research', 'current-parameters'], queryFn: ({ signal }) => readCurrentParameters(signal), enabled: active, retry: false, refetchOnWindowFocus: false })
  const history = useQuery({ queryKey: ['research', 'evaluations', page], queryFn: ({ signal }) => readEvaluations(page, signal), enabled: active, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data?.items.some(evaluationActive) ? 5000 : false })
  const job = useQuery({ queryKey: ['research', 'evaluation', selected], queryFn: ({ signal }) => readEvaluation(selected!, signal), enabled: active && selected !== null, retry: false, refetchOnWindowFocus: false, refetchInterval: query => query.state.data && evaluationActive(query.state.data) ? 1000 : false })
  const resultId = job.data?.result_id
  const result = useQuery({ queryKey: ['research', 'evaluation-result', resultId, selected], queryFn: ({ signal }) => readEvaluationResult(resultId!, selected!, signal), enabled: Boolean(active && resultId && selected && job.data?.status === 'completed'), retry: false, staleTime: Infinity, refetchOnWindowFocus: false })
  const actions = useEvaluationActions(value => {
    setSelected(value.id); setPage(1)
    client.setQueryData(['research', 'evaluation', value.id], value)
    void client.invalidateQueries({ queryKey: ['research', 'evaluations'] })
  })
  const profile = parameters.data?.items.find(item => item.strategy === 'martin')?.parameters.version
  const refresh = () => { void parameters.refetch(); void history.refetch(); if (selected) void job.refetch(); if (resultId) void result.refetch() }
  return { page, setPage, selected, setSelected, parameters, history, job, result, actions, profile, refresh }
}
