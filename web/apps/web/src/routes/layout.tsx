import { Outlet, NavLink, useNavigate } from 'react-router'
import { MessageSquare, Briefcase, TrendingUp, Settings, LogOut, BarChart3, Moon, FileDown, BookOpen, Home, Github, Filter, Sun, Languages, type LucideIcon } from 'lucide-react'
import { supabase } from '@/lib/supabase'
import { useAuthStore } from '@/stores/auth'
import { MarketBar } from '@/components/market-bar'
import { usePreferences, type TranslationKey } from '@/lib/preferences'

const navItems = [
  { to: '/chat', icon: MessageSquare, labelKey: 'nav.chat' },
  { to: '/analysis', icon: BarChart3, labelKey: 'nav.analysis' },
  { to: '/screener', icon: Filter, labelKey: 'nav.screener' },
  { to: '/portfolio', icon: Briefcase, labelKey: 'nav.portfolio' },
  { to: '/tracking', icon: TrendingUp, labelKey: 'nav.tracking' },
  { to: '/tail-buy', icon: Moon, labelKey: 'nav.tailBuy' },
  { to: '/export', icon: FileDown, labelKey: 'nav.export' },
  { to: '/guide', icon: BookOpen, labelKey: 'nav.guide' },
  { to: '/settings', icon: Settings, labelKey: 'nav.settings' },
] satisfies { to: string; icon: LucideIcon; labelKey: TranslationKey }[]

const externalLinks = [
  { href: 'https://youngcan-wang.github.io/wyckoff-homepage/', icon: Home, labelKey: 'external.home' },
  { href: 'https://github.com/YoungCan-Wang/WyckoffTradingAgent', icon: Github, labelKey: 'external.github' },
] satisfies { href: string; icon: LucideIcon; labelKey: TranslationKey }[]

function PreferenceControls() {
  const { locale, setLocale, theme, toggleTheme, t } = usePreferences()
  const nextLocale = locale === 'zh-CN' ? 'en-US' : 'zh-CN'
  const ThemeIcon = theme === 'dark' ? Sun : Moon

  return (
    <div className="mb-3 flex gap-2 px-3">
      <button
        type="button"
        onClick={toggleTheme}
        title={theme === 'dark' ? t('prefs.light') : t('prefs.dark')}
        aria-label={t('prefs.theme')}
        className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-lg border border-border text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
      >
        <ThemeIcon size={14} />
        {theme === 'dark' ? t('prefs.light') : t('prefs.dark')}
      </button>
      <button
        type="button"
        onClick={() => setLocale(nextLocale)}
        title={locale === 'zh-CN' ? t('prefs.switchToEnglish') : t('prefs.switchToChinese')}
        aria-label={t('prefs.language')}
        className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-lg border border-border text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
      >
        <Languages size={14} />
        {locale === 'zh-CN' ? 'EN' : '中文'}
      </button>
    </div>
  )
}

function SidebarFooter({ email, onLogout }: { email: string; onLogout: () => void }) {
  const { t } = usePreferences()

  return (
    <div className="border-t border-border p-3">
      <PreferenceControls />
      {externalLinks.map(({ href, icon: Icon, labelKey }) => (
        <a
          key={href}
          href={href}
          target="_blank"
          rel="noopener noreferrer"
          className="mb-2 flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
        >
          <Icon size={14} />
          {t(labelKey)}
        </a>
      ))}
      <div className="mb-2 truncate px-3 text-[11px] text-muted-foreground">{email}</div>
      <button
        onClick={onLogout}
        className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
      >
        <LogOut size={15} />
        {t('action.logout')}
      </button>
    </div>
  )
}

export function AppLayout() {
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const { t } = usePreferences()

  async function handleLogout() {
    await supabase.auth.signOut()
    navigate('/login', { replace: true })
  }

  return (
    <div className="flex h-screen">
      <aside className="flex w-56 flex-col border-r border-border bg-sidebar">
        <div className="px-5 py-5">
          <h2 className="bg-gradient-to-r from-primary to-cyan-500 bg-clip-text text-xl font-bold tracking-tight text-transparent">
            Wyckoff
          </h2>
          <p className="mt-0.5 text-[11px] text-muted-foreground">{t('app.subtitle')}</p>
        </div>

        <nav className="flex-1 space-y-0.5 px-3">
          {navItems.map(({ to, icon: Icon, labelKey }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-all ${
                  isActive
                    ? 'bg-primary/10 font-medium text-primary shadow-sm'
                    : 'text-muted-foreground hover:bg-muted hover:text-foreground'
                }`
              }
            >
              <Icon size={18} />
              {t(labelKey)}
            </NavLink>
          ))}
        </nav>

        <SidebarFooter email={user?.email || 'dev@preview'} onLogout={handleLogout} />
      </aside>

      <div className="flex flex-1 flex-col overflow-hidden">
        <MarketBar />
        <main className="flex-1 overflow-auto bg-background">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
