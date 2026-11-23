import { useEffect, useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { cancelChatTurn, createChatTurn, readChatTurn, readChatTurns, ResearchApiError } from './api'
import { chatIsActive, type ChatTurn, type CreateChatTurn } from './chat-contract'

export function useResearchChat(reportId: string) {
  const client = useQueryClient()
  const [draft, setDraft] = useState('')
  const [conversation, setConversation] = useState<string | undefined>()
  const [page, setPage] = useState(1)
  const [activeId, setActiveId] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [uncertain, setUncertain] = useState(false)
  const [message, setMessage] = useState('')
  const guard = useRef(false)
  const request = useRef<{ body: CreateChatTurn; key: string } | null>(null)
  const key = ['research', 'chat-history', reportId, conversation, page]
  const history = useQuery({ queryKey: key, queryFn: ({ signal }) => readChatTurns(reportId, page, conversation, signal), retry: false, refetchOnWindowFocus: false })
  const leaf = useQuery({ queryKey: ['research', 'chat-history', reportId, conversation, 1], queryFn: ({ signal }) => readChatTurns(reportId, 1, conversation, signal), enabled: Boolean(conversation), retry: false, refetchOnWindowFocus: false, refetchInterval: query => chatIsActive(query.state.data?.items[0]) ? 1500 : false })
  const active = useQuery({ queryKey: ['research', 'chat-turn', activeId], queryFn: ({ signal }) => readChatTurn(activeId!, reportId, signal), enabled: Boolean(activeId), retry: false, refetchOnWindowFocus: false, refetchInterval: query => chatIsActive(query.state.data) ? 1500 : false })
  const latest = conversation ? leaf.data?.items[0] : undefined
  const running = chatIsActive(active.data) || (Boolean(activeId) && active.isPending) || chatIsActive(latest)
  useEffect(() => {
    if (active.data && !chatIsActive(active.data)) void client.invalidateQueries({ queryKey: ['research', 'chat-history', reportId] })
  }, [active.data?.status, activeId, client, reportId])
  const refresh = () => { void history.refetch(); if (conversation) void leaf.refetch(); if (activeId) void active.refetch() }
  async function send() {
    if (guard.current || (running && !request.current)) return
    if (!request.current) {
      if (!draft.trim() || draft.length > 6000 || (conversation && (!latest || leaf.error || leaf.isFetching))) return
      request.current = { body: { report_id: reportId, question: draft.trim(), ...(latest ? { parent_turn_id: latest.id } : {}) }, key: crypto.randomUUID() }
    }
    guard.current = true; setBusy(true); setMessage('')
    try {
      const result = await createChatTurn(request.current.body, request.current.key)
      request.current = null; setUncertain(false); setDraft(''); setConversation(result.conversation_id); setPage(1); setActiveId(result.id)
      client.setQueryData(['research', 'chat-turn', result.id], result)
      await client.invalidateQueries({ queryKey: ['research', 'chat-history', reportId] })
    } catch (error) {
      const unknown = error instanceof ResearchApiError && (error.status === 0 || error.code === 'invalid_response' || error.status >= 500)
      setUncertain(unknown)
      if (!unknown) request.current = null
      setMessage(error instanceof Error ? error.message : '问答提交未确认')
    } finally { guard.current = false; setBusy(false) }
  }
  async function cancel() {
    const identifier = chatIsActive(active.data) ? activeId : latest?.id
    if (!identifier || guard.current) return
    guard.current = true; setBusy(true); setMessage('')
    try {
      const result = await cancelChatTurn(identifier, reportId)
      client.setQueryData(['research', 'chat-turn', identifier], result)
      await client.invalidateQueries({ queryKey: ['research', 'chat-history', reportId] })
    } catch (error) { setMessage(error instanceof Error ? error.message : '取消结果未确认'); refresh() }
    finally { guard.current = false; setBusy(false) }
  }
  const select = (turn?: ChatTurn) => {
    if (busy || uncertain || running) return
    setConversation(turn?.conversation_id); setPage(1); setActiveId(turn && chatIsActive(turn) ? turn.id : null); setMessage('')
  }
  const loadDraft = (text: string) => {
    if (busy || uncertain) { setMessage('请先确认原提交，再载入新问题'); return }
    setDraft(text)
  }
  return { draft, setDraft, conversation, page, setPage, history, leaf, active, latest, running, busy, uncertain, message, showMessage: setMessage, send, cancel, refresh, select,
    loadDraft, canSend: !busy && !running && !uncertain && Boolean(draft.trim()) && draft.length <= 6000 && (!conversation || Boolean(latest && !leaf.error && !leaf.isFetching)),
  }
}
