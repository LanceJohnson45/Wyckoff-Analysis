import { useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { confirmTheme, proposeTheme, publishManual, submissionUnconfirmed } from './api'
import type { ConfirmTheme, CreateManualOverride, ThemeDefinition } from './input-contract'

type InputAction = { kind: 'proposal'; body: ThemeDefinition; key: string } | { kind: 'confirm'; identifier: string; body: ConfirmTheme } | { kind: 'manual'; body: CreateManualOverride; key: string }

export function useInputActions() {
  const client = useQueryClient()
  const pending = useRef<InputAction | null>(null)
  const guard = useRef(false)
  const [busy, setBusy] = useState(false)
  const [uncertain, setUncertain] = useState(false)
  const [message, setMessage] = useState('')
  async function submit(action?: InputAction): Promise<boolean> {
    if (guard.current || (!pending.current && !action)) return false
    const reconciling = pending.current !== null
    const original = pending.current || action!
    pending.current = original; guard.current = true; setBusy(true); setMessage('')
    try {
      if (original.kind === 'proposal') await proposeTheme(original.body, original.key)
      else if (original.kind === 'confirm') await confirmTheme(original.identifier, original.body)
      else await publishManual(original.body, original.key)
      pending.current = null; setUncertain(false)
      setMessage(original.kind === 'proposal' ? '主题建议已保存，尚未发布；请复核成员后人工确认。' : '人工修订已保存；新研究按生效日期消费，历史报告保持原版本。')
      void client.invalidateQueries({ queryKey: ['research', 'inputs'] })
      return true
    } catch (error) {
      const unknown = submissionUnconfirmed(error, reconciling)
      setUncertain(unknown)
      if (!unknown) pending.current = null
      setMessage(error instanceof Error ? error.message : '人工输入操作未确认')
      return false
    } finally { guard.current = false; setBusy(false) }
  }
  return { submit, busy, uncertain, blocked: busy || uncertain, message, showMessage: setMessage }
}
export type InputActions = ReturnType<typeof useInputActions>
