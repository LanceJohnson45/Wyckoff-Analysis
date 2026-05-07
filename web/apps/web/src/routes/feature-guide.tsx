import { BarChart3, Bot, Briefcase, Download, Filter, MessageSquare, Moon, RadioTower, Settings, TrendingUp, type LucideIcon } from 'lucide-react'
import { usePreferences, type TranslationKey } from '@/lib/preferences'

const workflows = [
  {
    icon: MessageSquare,
    titleKey: 'guide.workflow.chat.title',
    descKey: 'guide.workflow.chat.desc',
  },
  {
    icon: BarChart3,
    titleKey: 'guide.workflow.analysis.title',
    descKey: 'guide.workflow.analysis.desc',
  },
  {
    icon: Filter,
    titleKey: 'guide.workflow.screener.title',
    descKey: 'guide.workflow.screener.desc',
  },
  {
    icon: Briefcase,
    titleKey: 'guide.workflow.portfolio.title',
    descKey: 'guide.workflow.portfolio.desc',
  },
] satisfies { icon: LucideIcon; titleKey: TranslationKey; descKey: TranslationKey }[]

const tools = [
  { nameKey: 'guide.tool.market', detailKey: 'guide.tool.market.detail' },
  { nameKey: 'guide.tool.tracking', detailKey: 'guide.tool.tracking.detail' },
  { nameKey: 'guide.tool.signal', detailKey: 'guide.tool.signal.detail' },
  { nameKey: 'guide.tool.tail', detailKey: 'guide.tool.tail.detail' },
  { nameKey: 'guide.tool.export', detailKey: 'guide.tool.export.detail' },
  { nameKey: 'guide.tool.model', detailKey: 'guide.tool.model.detail' },
] satisfies { nameKey: TranslationKey; detailKey: TranslationKey }[]

const playbooks = [
  {
    labelKey: 'guide.playbook.pre',
    icon: RadioTower,
    textKey: 'guide.playbook.pre.text',
  },
  {
    labelKey: 'guide.playbook.mid',
    icon: TrendingUp,
    textKey: 'guide.playbook.mid.text',
  },
  {
    labelKey: 'guide.playbook.tail',
    icon: Moon,
    textKey: 'guide.playbook.tail.text',
  },
  {
    labelKey: 'guide.playbook.review',
    icon: Download,
    textKey: 'guide.playbook.review.text',
  },
] satisfies { labelKey: TranslationKey; icon: LucideIcon; textKey: TranslationKey }[]

export function FeatureGuidePage() {
  const { t } = usePreferences()

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-8 p-6">
      <header className="border-b border-border pb-5">
        <div className="flex items-center gap-3">
          <span className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
            <Bot size={22} />
          </span>
          <div>
            <h1 className="text-xl font-semibold">{t('guide.title')}</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              {t('guide.subtitle')}
            </p>
          </div>
        </div>
      </header>

      <section>
        <div className="mb-4 flex items-end justify-between gap-4">
          <div>
            <h2 className="text-base font-semibold">{t('guide.core')}</h2>
            <p className="mt-1 text-sm text-muted-foreground">{t('guide.coreDesc')}</p>
          </div>
        </div>
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          {workflows.map(({ icon: Icon, titleKey, descKey }) => (
            <article key={titleKey} className="rounded-lg border border-border bg-background p-4 shadow-sm shadow-primary/5">
              <Icon className="mb-3 text-primary" size={20} />
              <h3 className="text-sm font-semibold">{t(titleKey)}</h3>
              <p className="mt-2 text-sm leading-6 text-muted-foreground">{t(descKey)}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[1fr_380px]">
        <div>
          <h2 className="mb-4 text-base font-semibold">{t('guide.modules')}</h2>
          <div className="overflow-hidden rounded-lg border border-border bg-background">
            <table className="w-full text-sm">
              <thead className="bg-muted/60 text-left text-xs text-muted-foreground">
                <tr>
                  <th className="px-4 py-3 font-medium">{t('guide.module')}</th>
                  <th className="px-4 py-3 font-medium">{t('guide.description')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {tools.map((tool) => (
                  <tr key={tool.nameKey}>
                    <td className="whitespace-nowrap px-4 py-3 font-medium">{t(tool.nameKey)}</td>
                    <td className="px-4 py-3 leading-6 text-muted-foreground">{t(tool.detailKey)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <aside>
          <h2 className="mb-4 text-base font-semibold">{t('guide.rhythm')}</h2>
          <div className="space-y-3">
            {playbooks.map(({ icon: Icon, labelKey, textKey }) => (
              <div key={labelKey} className="rounded-lg border border-border bg-sidebar p-4">
                <div className="flex items-center gap-2 text-sm font-semibold">
                  <Icon size={16} className="text-primary" />
                  {t(labelKey)}
                </div>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">{t(textKey)}</p>
              </div>
            ))}
          </div>
        </aside>
      </section>

      <section className="rounded-lg border border-border bg-primary/5 p-5">
        <div className="flex items-start gap-3">
          <Settings className="mt-0.5 text-primary" size={18} />
          <div>
            <h2 className="text-sm font-semibold">{t('guide.configEntry')}</h2>
            <p className="mt-2 text-sm leading-6 text-muted-foreground">
              {t('guide.configDesc')}
            </p>
          </div>
        </div>
      </section>
    </div>
  )
}
