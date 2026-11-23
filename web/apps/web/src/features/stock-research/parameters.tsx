import { useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { publishParameterVersion, readCurrentParameters, readFrozenParameters, readParameterVersions, registerParameterVersion, ResearchApiError, type Strategy } from './api'
import { parseParameterDraft, type ParameterSet, type StrategyParameters } from './parameter-contract'
import { strategyLabels } from './state'

const button = 'rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted disabled:opacity-40'

export function ResearchParameters({ runId }: { runId?: string }) {
  const [opened, setOpened] = useState(false)
  return <details className="rounded-2xl border border-border bg-card p-5" onToggle={event => { if (event.currentTarget.open) setOpened(true) }}>
    <summary className="cursor-pointer text-sm font-semibold">研究参数与版本</summary>
    {opened && <ParameterWorkspace runId={runId} />}
  </details>
}

function ParameterWorkspace({ runId }: { runId?: string }) {
  const current = useQuery({ queryKey: ['research', 'current-parameters'], queryFn: ({ signal }) => readCurrentParameters(signal), retry: false, refetchOnWindowFocus: false })
  const frozen = useQuery({ queryKey: ['research', 'run-parameters', runId], queryFn: ({ signal }) => readFrozenParameters(runId!, signal), enabled: Boolean(runId), retry: false, refetchOnWindowFocus: false })
  return <div className="mt-4 space-y-5">
    <p className="text-xs text-muted-foreground">当前配置用于下一次研究；发布不改写历史运行或当天已采用的名单。登记版本后，再单独选择发布。</p>
    {current.isPending && <p role="status" className="text-xs">正在读取当前参数…</p>}
    {current.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{current.error.message}<button type="button" className="ml-3 underline" onClick={() => { void current.refetch() }}>重新读取当前参数</button></p>}
    {current.data && <div className="grid items-start gap-4 xl:grid-cols-2">{current.data.items.map(item => <StrategyEditor key={item.strategy} current={item} available={!current.error && !current.isFetching} />)}</div>}
    <section className="space-y-3 border-t border-border pt-4" aria-label="本报告冻结参数"><h3 className="text-sm font-medium">本报告冻结参数</h3>
      {!runId && <p className="text-xs text-muted-foreground">选择研究报告后，可核对该次实际使用的完整参数。</p>}
      {runId && frozen.isPending && <p role="status" className="text-xs">正在读取该次冻结参数…</p>}
      {runId && frozen.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{frozen.error.message}<button type="button" className="ml-3 underline" onClick={() => { void frozen.refetch() }}>重试冻结参数</button></p>}
      {runId && frozen.data && <><p className="break-all text-[11px] text-muted-foreground">固定运行 {runId}</p>{frozen.data.items.map(item => <ParameterSummary key={item.strategy} item={item} frozen />)}</>}
    </section>
  </div>
}

export function ParameterSummary({ item, frozen = false }: { item: StrategyParameters; frozen?: boolean }) {
  return <div className="min-w-0 space-y-2 rounded-xl bg-muted/30 p-3" aria-label={`${strategyLabels[item.strategy]}${frozen ? '冻结' : '当前'}参数`}>
    <p className="text-xs font-medium">{strategyLabels[item.strategy]} · {item.parameters.version}</p>
    <p className="text-xs text-muted-foreground">{frozen ? '运行冻结' : '当前生效'} · {item.source === 'published' ? '已发布版本' : 'Operator基线'} · 修订 {item.revision}</p>
    <details><summary className="cursor-pointer text-xs text-muted-foreground">完整值与参数摘要</summary><pre className="mt-2 max-h-72 overflow-auto whitespace-pre-wrap break-all text-[11px]">{JSON.stringify(item.parameters, null, 2)}</pre><p className="mt-2 break-all text-[11px] text-muted-foreground">{item.parameter_hash}</p></details>
  </div>
}

function useParameterActions(strategy: Strategy, revision: number, refreshVersions: () => void) {
  const client = useQueryClient()
  const guard = useRef(false)
  const [busy, setBusy] = useState(false)
  const [locked, setLocked] = useState(false)
  const [message, setMessage] = useState('')
  async function act(action: 'register' | 'publish' | 'refresh', value: string) {
    if (guard.current || (action === 'publish' && locked)) return
    guard.current = true; setBusy(true); setMessage('')
    try {
      if (action === 'register') {
        const saved = await registerParameterVersion(strategy, parseParameterDraft(value, strategy))
        refreshVersions(); setMessage(`已登记 ${saved.parameters.version}，尚未发布`)
      } else if (action === 'publish') {
        await publishParameterVersion(strategy, value, revision)
        setLocked(true)
        await client.invalidateQueries({ queryKey: ['research', 'current-parameters'] })
        setMessage('发布已确认。请读取当前配置，核对生效版本。')
      } else {
        await client.fetchQuery({ queryKey: ['research', 'current-parameters'], queryFn: ({ signal }) => readCurrentParameters(signal), staleTime: 0 })
        setLocked(false); setMessage('已读取当前配置；草稿保持原样')
      }
    } catch (error) {
      if (action === 'publish') setLocked(true)
      const detail = error instanceof Error ? error.message : '参数操作失败'
      setMessage(error instanceof ResearchApiError && error.status === 409 ? `${detail}。请先读取当前配置，核对后再发布。` : detail)
    } finally { guard.current = false; setBusy(false) }
  }
  return { act, busy, locked, message }
}

function StrategyEditor({ current, available }: { current: StrategyParameters; available: boolean }) {
  const [draft, setDraft] = useState(() => JSON.stringify(current.parameters, null, 2))
  const [page, setPage] = useState(1)
  const versions = useQuery({ queryKey: ['research', 'parameter-versions', current.strategy, page], queryFn: ({ signal }) => readParameterVersions(current.strategy, page, signal), retry: false, refetchOnWindowFocus: false })
  const client = useQueryClient()
  const refreshVersions = () => { void client.invalidateQueries({ queryKey: ['research', 'parameter-versions', current.strategy] }) }
  const action = useParameterActions(current.strategy, current.revision, refreshVersions)
  return <section className="min-w-0 space-y-3 rounded-xl border border-border p-4" aria-label={`${strategyLabels[current.strategy]}参数维护`}>
    <ParameterSummary item={current} />
    <div className="flex flex-wrap gap-2"><button type="button" disabled={action.busy} className={button} onClick={() => { void action.act('refresh', '') }}>读取当前配置</button><button type="button" disabled={action.busy || !available} className={button} onClick={() => setDraft(JSON.stringify(current.parameters, null, 2))}>载入当前参数到草稿</button></div>
    <form className="space-y-3" onSubmit={event => { event.preventDefault(); void action.act('register', draft) }}>
      <label className="block space-y-2 text-xs"><span>参数草稿 · {strategyLabels[current.strategy]}</span><textarea aria-label={`${strategyLabels[current.strategy]}参数JSON草稿`} spellCheck={false} value={draft} onChange={event => setDraft(event.target.value)} disabled={action.busy} rows={12} className="w-full rounded-lg border border-border bg-background p-3 font-mono text-[11px] leading-relaxed disabled:opacity-50" /></label>
      <p className="text-[11px] leading-relaxed text-muted-foreground">保留完整字段，修改version及需要调整的值。新名称登记新版本；同名不同内容会被拒绝。周期、权重及条件由研究后台严格校验。</p>
      <button type="submit" disabled={action.busy} className={button}>{action.busy ? '正在处理…' : '登记草稿为新版本'}</button>
    </form>
    {action.message && <p role="status" className="text-xs leading-relaxed text-amber-700 dark:text-amber-300">{action.message}</p>}
    {action.locked && <p className="text-xs text-muted-foreground">发布操作已锁定；读取当前配置成功后可继续。</p>}
    <div className="space-y-3 border-t border-border pt-3"><h4 className="text-xs font-medium">已登记版本 · 发布基于当前修订 {current.revision}</h4>
      {versions.isPending && <p role="status" className="text-xs">正在读取版本…</p>}
      {versions.error && <p role="alert" className="text-xs text-amber-700 dark:text-amber-300">{versions.error.message}<button type="button" className="ml-3 underline" onClick={refreshVersions}>重试版本查询</button></p>}
      {versions.data && <>{versions.data.items.map(item => <RegisteredVersion key={item.id} item={item} active={current.parameter_set_id === item.id} disabled={action.busy || action.locked || !available} onLoad={() => setDraft(JSON.stringify(item.parameters, null, 2))} onPublish={() => { void action.act('publish', item.id) }} />)}
        {versions.data.total === 0 && <p className="text-xs text-muted-foreground">尚无已登记版本。</p>}
        <div className="flex justify-between gap-2 text-xs text-muted-foreground"><button type="button" disabled={page === 1 || action.busy} onClick={() => setPage(page - 1)} className="disabled:opacity-40">上一页</button><span>{page} 页 · 共 {versions.data.total} 个</span><button type="button" disabled={page * versions.data.page_size >= versions.data.total || action.busy} onClick={() => setPage(page + 1)} className="disabled:opacity-40">下一页</button></div>
      </>}
    </div>
  </section>
}

function RegisteredVersion({ item, active, disabled, onLoad, onPublish }: { item: ParameterSet; active: boolean; disabled: boolean; onLoad: () => void; onPublish: () => void }) {
  return <article className="space-y-2 rounded-lg bg-muted/30 p-3">
    <p className="break-all text-xs font-medium">{item.parameters.version}{active && <span className="ml-2 text-primary">当前生效</span>}</p>
    <details><summary className="cursor-pointer text-xs text-muted-foreground">查看此版本完整值</summary><pre className="mt-2 max-h-64 overflow-auto whitespace-pre-wrap break-all text-[11px]">{JSON.stringify(item.parameters, null, 2)}</pre><p className="mt-2 break-all text-[11px]">{item.parameter_hash}</p></details>
    <div className="flex flex-wrap gap-2"><button type="button" className={button} onClick={onLoad} disabled={disabled}>载入到草稿</button><button type="button" className={button} onClick={onPublish} disabled={disabled || active}>发布此版本</button></div>
  </article>
}
