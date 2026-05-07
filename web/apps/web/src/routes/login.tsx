import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router'
import { Languages, Moon, Sun } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { useAuthStore } from '@/stores/auth'
import { usePreferences } from '@/lib/preferences'

export function LoginPage() {
  const navigate = useNavigate()
  const setAuth = useAuthStore((s) => s.setAuth)
  const { locale, setLocale, theme, toggleTheme, t } = usePreferences()
  const ThemeIcon = theme === 'dark' ? Sun : Moon
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [isRegister, setIsRegister] = useState(false)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [checkingSession, setCheckingSession] = useState(true)

  useEffect(() => {
    let active = true

    supabase.auth
      .getSession()
      .then(({ data: { session } }) => {
        if (!active) {
          return
        }
        if (session) {
          setAuth(session.user, session)
          navigate('/', { replace: true })
          return
        }
        setCheckingSession(false)
      })
      .catch(() => {
        if (active) {
          setCheckingSession(false)
        }
      })

    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        if (!active || !session) {
          return
        }
        setAuth(session.user, session)
        navigate('/', { replace: true })
      },
    )

    return () => {
      active = false
      subscription.unsubscribe()
    }
  }, [navigate, setAuth])

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)

    try {
      if (isRegister) {
        const { error } = await supabase.auth.signUp({ email, password })
        if (error) throw error
      } else {
        const { data, error } = await supabase.auth.signInWithPassword({ email, password })
        if (error) throw error
        if (data.session) {
          setAuth(data.user, data.session)
        }
      }
      navigate('/', { replace: true })
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : t('login.operationFailed'))
    } finally {
      setLoading(false)
    }
  }

  if (checkingSession) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      </div>
    )
  }

  return (
    <div className="relative flex min-h-screen items-center justify-center bg-background px-4">
      <div className="absolute right-4 top-4 flex gap-2">
        <button
          type="button"
          onClick={toggleTheme}
          className="flex h-9 w-9 items-center justify-center rounded-lg border border-border text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label={t('prefs.theme')}
        >
          <ThemeIcon size={16} />
        </button>
        <button
          type="button"
          onClick={() => setLocale(locale === 'zh-CN' ? 'en-US' : 'zh-CN')}
          className="flex h-9 items-center gap-1.5 rounded-lg border border-border px-2.5 text-xs text-muted-foreground hover:bg-muted hover:text-foreground"
          aria-label={t('prefs.language')}
        >
          <Languages size={15} />
          {locale === 'zh-CN' ? 'EN' : '中文'}
        </button>
      </div>
      <div className="w-full max-w-sm rounded-2xl border border-border bg-background p-8 shadow-xl shadow-primary/5">
        <div className="mb-8 text-center">
          <h1 className="bg-gradient-to-r from-primary to-cyan-500 bg-clip-text text-3xl font-bold text-transparent">
            Wyckoff
          </h1>
          <p className="mt-1 text-sm text-muted-foreground">{t('app.subtitle')}</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="mb-1.5 block text-sm font-medium text-foreground">{t('login.email')}</label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              className="w-full rounded-xl border border-border bg-muted/30 px-4 py-2.5 text-sm outline-none transition-all focus:border-primary focus:ring-2 focus:ring-primary/20"
              placeholder="your@email.com"
              required
            />
          </div>

          <div>
            <label className="mb-1.5 block text-sm font-medium text-foreground">{t('login.password')}</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full rounded-xl border border-border bg-muted/30 px-4 py-2.5 text-sm outline-none transition-all focus:border-primary focus:ring-2 focus:ring-primary/20"
              placeholder="••••••••"
              required
              minLength={6}
            />
          </div>

          {error && (
            <p className="rounded-lg bg-red-50 px-3 py-2 text-sm text-red-600 dark:bg-red-500/10 dark:text-red-200">{error}</p>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-xl bg-gradient-to-r from-primary to-cyan-500 px-4 py-2.5 text-sm font-medium text-white shadow-lg shadow-primary/25 transition-all hover:shadow-xl hover:shadow-primary/30 disabled:opacity-50"
          >
            {loading ? t('login.processing') : isRegister ? t('login.register') : t('login.submit')}
          </button>
        </form>

        <p className="mt-5 text-center text-sm text-muted-foreground">
          {isRegister ? t('login.hasAccount') : t('login.noAccount')}
          <button
            onClick={() => setIsRegister(!isRegister)}
            className="ml-1 font-medium text-primary hover:underline"
          >
            {isRegister ? t('login.submit') : t('login.register')}
          </button>
        </p>
      </div>
    </div>
  )
}
