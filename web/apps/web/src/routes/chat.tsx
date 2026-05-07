import { useState, useRef, useEffect, useCallback, memo } from 'react'
import { Send, RotateCcw, ChevronDown, ChevronRight, Wrench, Brain } from 'lucide-react'
import { useAuthStore } from '@/stores/auth'
import { loadLLMConfig, loadAllModels, runChatAgentStream, type LLMConfig, type ModelOption, type StepInfo } from '@/lib/chat-agent'
import { MarkdownContent } from '@/components/markdown'
import { usePreferences, type TranslationKey } from '@/lib/preferences'

const TOOL_LABEL_KEYS: Record<string, TranslationKey> = {
  search_stock: 'tool.search_stock',
  view_portfolio: 'tool.view_portfolio',
  market_overview: 'tool.market_overview',
  query_recommendations: 'tool.query_recommendations',
  query_tail_buy: 'tool.query_tail_buy',
  plan_portfolio_update: 'tool.plan_portfolio_update',
  execute_portfolio_update: 'tool.execute_portfolio_update',
  analyze_stock: 'tool.analyze_stock',
  screen_stocks: 'tool.screen_stocks',
  generate_ai_report: 'tool.generate_ai_report',
  generate_strategy_decision: 'tool.generate_strategy_decision',
}

interface Message {
  role: 'user' | 'assistant'
  content: string
  isError?: boolean
  steps?: StepInfo[]
}

function StepsCollapsible({ steps }: { steps: StepInfo[] }) {
  const [expanded, setExpanded] = useState(false)
  const { t } = usePreferences()

  if (steps.length === 0) return null

  const toolCalls = steps.filter((s) => s.type === 'tool_call')
  const summary = toolCalls.length > 0
    ? t('chat.toolCalls', { count: toolCalls.length })
    : t('chat.reasoningSteps', { count: steps.length })

  return (
    <div className="mb-2">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1 text-[11px] text-muted-foreground/70 hover:text-muted-foreground transition-colors"
      >
        <ChevronRight size={12} className={`transition-transform ${expanded ? 'rotate-90' : ''}`} />
        <span>{summary}</span>
      </button>
      {expanded && (
        <div className="mt-1.5 ml-3 space-y-1 border-l-2 border-border/50 pl-2.5">
          {steps.map((step, i) => (
            <div key={i} className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
              {step.type === 'tool_call' ? (
                <>
                  <Wrench size={10} className="text-amber-500" />
                  <span>{formatToolName(step.toolName, t)}</span>
                </>
              ) : (
                <>
                  <Brain size={10} className="text-blue-500" />
                  <span className="line-clamp-1">{step.text?.slice(0, 80)}…</span>
                </>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

const MessageBubble = memo(function MessageBubble({ msg }: { msg: Message }) {
  return (
    <div className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[80%] rounded-2xl px-4 py-2.5 text-sm ${
          msg.role === 'user'
            ? 'bg-primary text-primary-foreground whitespace-pre-wrap'
            : msg.isError
              ? 'border border-red-200 bg-red-50 text-red-700 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-200'
              : 'bg-muted text-foreground'
        }`}
      >
        {msg.role === 'user' ? (
          msg.content
        ) : (
          <>
            {msg.steps && msg.steps.length > 0 && <StepsCollapsible steps={msg.steps} />}
            <MarkdownContent content={msg.content} />
          </>
        )}
      </div>
    </div>
  )
})

export function ChatPage() {
  const user = useAuthStore((s) => s.user)
  const { t } = usePreferences()
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [llmConfig, setLlmConfig] = useState<LLMConfig | null>(null)
  const [models, setModels] = useState<ModelOption[]>([])
  const [showModelPicker, setShowModelPicker] = useState(false)
  const [liveSteps, setLiveSteps] = useState<StepInfo[]>([])
  const [streamingText, setStreamingText] = useState('')
  const scrollRef = useRef<HTMLDivElement>(null)
  const abortRef = useRef(false)
  const pickerRef = useRef<HTMLDivElement>(null)
  const scrollRafRef = useRef(0)

  useEffect(() => {
    if (user) {
      loadLLMConfig(user.id).then(setLlmConfig)
      loadAllModels(user.id).then(setModels)
    }
  }, [user])

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (pickerRef.current && !pickerRef.current.contains(e.target as Node)) {
        setShowModelPicker(false)
      }
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [])

  const scrollToBottom = useCallback(() => {
    cancelAnimationFrame(scrollRafRef.current)
    scrollRafRef.current = requestAnimationFrame(() => {
      scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' })
    })
  }, [])

  useEffect(() => { scrollToBottom() }, [messages, liveSteps, scrollToBottom])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!input.trim() || loading) return

    if (!llmConfig) {
      setError(t('chat.configureLLM'))
      return
    }

    const userMsg: Message = { role: 'user', content: input.trim() }
    const newMessages = [...messages, userMsg]
    setMessages(newMessages)
    setInput('')
    setError('')
    setLoading(true)
    setLiveSteps([])
    setStreamingText('')
    abortRef.current = false

    const chatHistory = newMessages
      .filter((m) => !m.isError)
      .map((m) => ({
        role: m.role as 'user' | 'assistant',
        content: m.content,
      }))

    await runChatAgentStream(
      llmConfig,
      user!.id,
      chatHistory,
      {
        onStep: (step) => {
          if (abortRef.current) return
          setLiveSteps((prev) => [...prev, step])
          setStreamingText('')
        },
        onTextDelta: (delta) => {
          if (abortRef.current) return
          setStreamingText((prev) => prev + delta)
          scrollToBottom()
        },
        onFinish: (finalText, steps) => {
          if (abortRef.current) return
          if (finalText) {
            setMessages((prev) => [...prev, { role: 'assistant', content: finalText, steps }])
          }
          setStreamingText('')
          setLiveSteps([])
          setLoading(false)
        },
        onError: (err) => {
          const msg = err.message || t('chat.requestFailed')
          setError(msg)
          setMessages((prev) => [...prev, { role: 'assistant', content: `⚠️ ${msg}`, isError: true }])
          setStreamingText('')
          setLiveSteps([])
          setLoading(false)
        },
      },
    )
  }

  function handleNewChat() {
    abortRef.current = true
    setMessages([])
    setLiveSteps([])
    setError('')
    setLoading(false)
  }

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-6 py-3">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold">{t('chat.title')}</h1>
          {llmConfig && (
            <div className="relative" ref={pickerRef}>
              <button
                onClick={() => setShowModelPicker(!showModelPicker)}
                className="flex items-center gap-1 rounded-full bg-indigo-50 px-2.5 py-0.5 text-[11px] text-indigo-700 transition-colors hover:bg-indigo-100 dark:bg-indigo-500/10 dark:text-indigo-200 dark:hover:bg-indigo-500/20"
              >
                {llmConfig.model}
                <ChevronDown size={10} />
              </button>
              {showModelPicker && models.length > 0 && (
                <div className="absolute left-0 top-full z-50 mt-1 w-56 rounded-lg border border-border bg-background shadow-lg">
                  {models.map((m) => (
                    <button
                      key={`${m.provider}-${m.model}`}
                      onClick={() => {
                        setLlmConfig({ api_key: m.api_key, model: m.model, base_url: m.base_url })
                        setShowModelPicker(false)
                      }}
                      className={`flex w-full items-center justify-between px-3 py-2 text-left text-xs hover:bg-muted/50 ${
                        m.model === llmConfig.model ? 'bg-muted/30 font-medium' : ''
                      }`}
                    >
                      <span>{m.model}</span>
                      <span className="text-muted-foreground">{m.label}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}
          {!llmConfig && user && (
            <span className="rounded-full bg-amber-50 px-2 py-0.5 text-[11px] text-amber-700 dark:bg-amber-500/10 dark:text-amber-200">
              {t('chat.noApiKey')}
            </span>
          )}
        </div>
        <button
          onClick={handleNewChat}
          className="flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 text-sm text-muted-foreground hover:bg-muted/50"
        >
          <RotateCcw size={14} />
          {t('chat.newChat')}
        </button>
      </div>

      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-auto px-6 py-4">
        {messages.length === 0 && !loading ? (
          <div className="flex h-full flex-col items-center justify-center text-muted-foreground">
            <div className="mb-4 text-4xl">📈</div>
            <p className="text-sm font-medium">{t('chat.emptyTitle')}</p>
            <p className="mt-2 text-xs text-muted-foreground">{t('chat.tryAsk')}</p>
            <div className="mt-3 flex flex-wrap justify-center gap-2">
              {[
                t('chat.prompt.portfolio'),
                t('chat.prompt.market'),
                t('chat.prompt.recent'),
                t('chat.prompt.search'),
                t('chat.prompt.screen'),
                t('chat.prompt.strategy'),
              ].map((q) => (
                <button
                  key={q}
                  onClick={() => setInput(q)}
                  className="rounded-full border border-border px-3 py-1 text-xs text-muted-foreground hover:bg-muted/50"
                >
                  {q}
                </button>
              ))}
            </div>
            <div className="mt-8 rounded-lg border border-dashed border-border/60 px-4 py-2.5 text-center">
              <p className="text-[11px] text-muted-foreground/70">
                {t('chat.fullVersionPrefix')} ·{' '}
                <code className="rounded bg-muted px-1 py-0.5 text-[10px]">curl -fsSL https://raw.githubusercontent.com/YoungCan-Wang/Wyckoff-Analysis/main/install.sh | bash</code>{' '}
                {t('chat.unlockFull')}
              </p>
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            {messages.map((msg, i) => (
              <MessageBubble key={i} msg={msg} />
            ))}

            {/* Live streaming */}
            {loading && (
              <div className="flex justify-start">
                <div className="max-w-[80%] rounded-2xl bg-muted px-4 py-2.5 text-sm text-foreground">
                  {liveSteps.length > 0 && (
                    <div className="mb-2 space-y-1">
                      {liveSteps.map((step, i) => (
                        <div key={i} className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
                          {step.type === 'tool_call' ? (
                            <>
                              <Wrench size={10} className="text-amber-500" />
                              <span>✓ {formatToolName(step.toolName, t)}</span>
                            </>
                          ) : (
                            <>
                              <Brain size={10} className="text-blue-500" />
                              <span className="line-clamp-1">{step.text?.slice(0, 60)}…</span>
                            </>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                  {streamingText ? (
                    <MarkdownContent content={streamingText} />
                  ) : (
                    <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
                      <span className="inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-primary" />
                      <span>{liveSteps.length > 0 ? t('chat.generating') : t('chat.thinking')}</span>
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Error */}
      {error && (
        <div className="mx-6 mb-2 rounded-lg bg-red-50 px-3 py-2 text-xs text-red-700 dark:bg-red-500/10 dark:text-red-200">{error}</div>
      )}

      {/* Input */}
      <div className="border-t border-border px-6 py-4">
        <form onSubmit={handleSubmit} className="flex items-center gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={t('chat.placeholder')}
            className="flex-1 rounded-xl border border-border bg-background px-4 py-2.5 text-sm outline-none focus:ring-2 focus:ring-ring/20"
          />
          <button
            type="submit"
            disabled={!input.trim() || loading}
            className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary text-primary-foreground disabled:opacity-40"
          >
            <Send size={16} />
          </button>
        </form>
      </div>
    </div>
  )
}

function formatToolName(
  toolName: string | undefined,
  t: (key: TranslationKey) => string,
): string {
  if (!toolName) return ''
  const labelKey = TOOL_LABEL_KEYS[toolName]
  return labelKey ? t(labelKey) : toolName
}
