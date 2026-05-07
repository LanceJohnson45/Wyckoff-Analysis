import { useState, useEffect } from 'react'
import { usePreferences } from '@/lib/preferences'

const QUOTES = {
  'zh-CN': [
    '市场的本质是顺势而为，乘风而上，顺水推舟。',
    '不要问"为什么涨"，要问"谁在买"。',
    '成交量是主力的呼吸，价格只是它的影子。',
    '耐心等待，直到供需关系明确告诉你答案。',
    '每一次放量都是主力在行动，每一次缩量都是散户在犹豫。',
    '市场从不说谎，但它经常保持沉默。',
    '判断趋势不难，难的是等待趋势确认。',
    '价格在阻力位前的表现，比阻力位本身更重要。',
    '春天来临之前，总有最后一次寒流。',
    '综合人控制着一切，你要做的是跟上他的步伐。',
  ],
  'en-US': [
    'The market rewards alignment with the prevailing current.',
    'Do not only ask why it rose. Ask who was buying.',
    'Volume is the operator breathing; price is the shadow it leaves.',
    'Wait until supply and demand make the answer visible.',
    'Every surge in volume is an action; every quiet pullback is a clue.',
    'The market does not lie, but it often stays quiet.',
    'Reading trend is easy. Waiting for confirmation is the hard part.',
    'How price behaves near resistance matters more than the level itself.',
    'Before spring arrives, there is often one final cold wave.',
    'The Composite Man controls the board. Your job is to move with him.',
  ],
} as const

export function WyckoffLoading({ size = 'md' }: { size?: 'sm' | 'md' }) {
  const { locale } = usePreferences()
  const quotes = QUOTES[locale]
  const [idx, setIdx] = useState(() => Math.floor(Math.random() * quotes.length))

  useEffect(() => {
    const timer = setInterval(() => {
      setIdx((i) => (i + 1) % quotes.length)
    }, 4000)
    return () => clearInterval(timer)
  }, [quotes.length])

  if (size === 'sm') {
    return (
      <span className="flex items-center gap-2 text-muted-foreground">
        <span className="inline-block h-3.5 w-3.5 animate-spin rounded-full border-2 border-primary border-t-transparent" />
        <span className="text-xs italic">{quotes[idx]}</span>
      </span>
    )
  }

  return (
    <div className="flex h-full flex-col items-center justify-center gap-3 text-muted-foreground">
      <span className="inline-block h-6 w-6 animate-spin rounded-full border-2 border-primary border-t-transparent" />
      <span className="text-xs italic">{quotes[idx]}</span>
    </div>
  )
}
