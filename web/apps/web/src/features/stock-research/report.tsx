import type { ResearchPoint, ResearchReport, ResearchRun, SelectionItem, ReviewItem, Strategy } from './api'
import { statusLabels, strategyLabels } from './state'

const errorLabels: Record<string, string> = {
  deadline_exceeded: '阶段超时', run_deadline_exceeded: '运行预算已耗尽', transient_provider_error: '模型连接暂时异常',
  provider_rate_limit: '模型额度或限流', max_tokens: '模型输出截断', invalid_output: '模型结论未通过证据校验',
  tool_budget_exceeded: '证据查询预算已耗尽', boundary_or_protocol_failed: '执行边界校验失败', executor_unavailable: '研究执行器不可用',
  executor_configuration_failed: '执行配置未就绪', execution_version_changed: '执行版本已改变，请重新分析',
  previous_executor_stopped: '上次执行中断', stage_execution_failed: '阶段执行失败', transport_error: '执行连接中断',
  cancelled: '已取消', api_shutdown: '服务关闭后待恢复', cleanup_failed: '执行收尾未确认',
}
export function failureLabel(code: string): string { return errorLabels[code] || '阶段出现未识别错误' }
export function nyTime(value: string): string { return new Intl.DateTimeFormat('zh-CN', { timeZone: 'America/New_York', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(value)) }

export function ResearchReportView({ report, onDiscuss }: { report: ResearchReport; onDiscuss: () => void }) {
  const run = report.run
  return <section className="space-y-5" aria-label="固定研究报告">
    <header className="rounded-2xl border border-border bg-card p-5">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div><p className="text-xs text-muted-foreground">美股研究 · 目标交易日</p><h2 className="mt-1 text-2xl font-semibold tabular-nums">{run.target_session}</h2></div>
        <div className="flex items-center gap-2 text-xs"><span className="rounded-full bg-muted px-3 py-1">{statusLabels[run.status]}</span>{run.late && <span className="rounded-full bg-amber-500/10 px-3 py-1 text-amber-700 dark:text-amber-300">迟到报告</span>}</div>
      </div>
      <div className="mt-4 flex flex-wrap gap-x-6 gap-y-2 text-xs text-muted-foreground"><span>实际行情 {run.as_of}</span><span>修订 {report.revision}</span><span>生成 {nyTime(report.create_time)} 纽约</span><span>期限 {nyTime(run.report_due)} 纽约</span></div>
      <p className="mt-3 text-xs text-muted-foreground">各策略独立初选，交叉评议保留不同意见。以下为研究观察，未取得交易执行许可。</p>
      <button type="button" onClick={onDiscuss} className="mt-4 rounded-lg border border-border px-3 py-2 text-xs hover:bg-muted">带此报告继续讨论</button>
    </header>
    <div className="grid gap-5 xl:grid-cols-2">{(['martin', 'wyckoff'] as const).map((strategy) => <StrategyReport key={strategy} run={run} strategy={strategy} />)}</div>
    <StageProgress run={run} />
    <DataQuality run={run} />
  </section>
}

function StrategyReport({ run, strategy }: { run: ResearchRun; strategy: Strategy }) {
  const selection = run.selections.find((item) => item.strategy === strategy)
  const stage = run.stages.find((item) => item.strategy === strategy && item.kind === 'select')
  return <section className="min-w-0 rounded-2xl border border-border bg-card p-5" aria-label={`${strategyLabels[strategy]}初选`}>
    <div className="mb-4 flex items-center justify-between gap-3"><h3 className="text-sm font-semibold">{strategyLabels[strategy]}</h3><span className="text-xs text-muted-foreground">{selection ? `${selection.decision.items.length} / 5 只` : stage ? statusLabels[stage.status] : '待执行'}</span></div>
    {!selection && <p className="text-sm text-muted-foreground">{stage?.error ? failureLabel(stage.error) : '正式初选尚未提交，量化候选不作为初选展示。'}</p>}
    {selection && selection.decision.items.length === 0 && <div className="rounded-xl bg-muted/50 p-4"><p className="text-sm">本策略保留空初选</p>{selection.decision.empty_reason && <p className="mt-2 text-xs text-muted-foreground">{selection.decision.empty_reason.text}</p>}</div>}
    {selection && <div className="space-y-4">{selection.decision.items.map((item) => {
      const company = run.companies.find((row) => row.symbol === item.symbol)
      const reviews = run.reviews.filter((review) => review.decision.target_selection_id === selection.id)
      return <article key={item.symbol} className="rounded-xl border border-border p-4">
        <h4 className="text-sm font-semibold">{company ? company.name : '公司名称缺失'} <code className="ml-2 text-xs text-muted-foreground">{item.symbol}</code></h4>
        <InitialReason item={item} />
        {reviews.map((review) => { const opinion = review.decision.items.find((row) => row.symbol === item.symbol); return opinion ? <CrossOpinion key={review.id} item={opinion} strategy={review.strategy} /> : null })}
        {reviews.length === 0 && <p className="mt-3 border-t border-border pt-3 text-xs text-muted-foreground">另一策略评议尚未提交；初选原文已保留。</p>}
      </article>
    })}</div>}
  </section>
}

function InitialReason({ item }: { item: SelectionItem }) {
  return <div className="mt-3 space-y-2 text-xs leading-relaxed">
    <p>{item.rationale.text}</p>
    {item.risks.map((point, index) => <p key={index} className="text-muted-foreground">风险 · {point.text}</p>)}
    {item.confirmation_conditions.map((point, index) => <p key={index}>待确认 · {point.text}</p>)}
    <EvidenceDetails points={[item.rationale, ...item.risks, ...item.confirmation_conditions]} claims={item.claims} />
  </div>
}

function CrossOpinion({ item, strategy }: { item: ReviewItem; strategy: Strategy }) {
  const labels = { support: '支持', concern: '质疑', insufficient_evidence: '证据不足' }
  return <div className="mt-3 space-y-2 border-t border-border pt-3 text-xs leading-relaxed" aria-label={`${strategyLabels[strategy]}交叉评议`}>
    <p className="font-medium">{strategyLabels[strategy]} · {labels[item.stance]}</p><p>{item.rationale.text}</p>
    {item.objections.map((point, index) => <p key={index} className="text-muted-foreground">异议 · {point.text}</p>)}
    {item.confirmation_conditions.map((point, index) => <p key={index}>待确认 · {point.text}</p>)}
    <EvidenceDetails points={[item.rationale, ...item.objections, ...item.confirmation_conditions]} claims={item.claims} />
    {item.data_limitations.length > 0 && <details className="text-muted-foreground"><summary className="cursor-pointer">评议数据限制</summary><ul className="mt-2 space-y-1">{item.data_limitations.map((text) => <li key={text}>{text}</li>)}</ul></details>}
  </div>
}

function EvidenceDetails({ points, claims }: { points: ResearchPoint[]; claims: SelectionItem['claims'] }) {
  const refs = [...new Set([...points.flatMap((point) => point.evidence_refs), ...claims.map((claim) => claim.ref)])]
  return <details className="text-muted-foreground"><summary className="cursor-pointer">固定证据 · {refs.length} 项引用</summary><div className="mt-2 space-y-2 break-all">
    {claims.map((claim) => <p key={claim.ref}><code className="text-[10px]">{claim.ref}</code><span className="ml-2 font-medium" title={String(claim.value)}>{claim.value === null ? '缺失' : typeof claim.value === 'boolean' ? claim.value ? '是' : '否' : typeof claim.value === 'number' ? claim.value.toLocaleString('zh-CN', { maximumFractionDigits: 6 }) : claim.value}</span></p>)}
    {refs.filter((ref) => !claims.some((claim) => claim.ref === ref)).map((ref) => <p key={ref}><code className="text-[10px]">{ref}</code></p>)}
  </div></details>
}

function StageProgress({ run }: { run: ResearchRun }) {
  return <section className="rounded-2xl border border-border bg-card p-5" aria-label="研究阶段状态"><h3 className="mb-3 text-sm font-semibold">运行状态</h3>
    {run.error_code && <p role="alert" className="mb-3 text-xs text-amber-700 dark:text-amber-300">{failureLabel(run.error_code)}</p>}
    <div className="grid gap-2 md:grid-cols-2">{run.stages.map((stage) => <div key={stage.id} className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-muted/40 px-3 py-2 text-xs"><span>{strategyLabels[stage.strategy]} · {stage.kind === 'select' ? '初选' : '互评'}</span><span>{statusLabels[stage.status]} · 尝试 {stage.attempt_count}</span>{stage.error && <p className="w-full text-amber-700 dark:text-amber-300">{failureLabel(stage.error)}</p>}</div>)}</div>
  </section>
}

function DataQuality({ run }: { run: ResearchRun }) {
  return <section className="rounded-2xl border border-border bg-card p-5" aria-label="研究数据质量"><h3 className="text-sm font-semibold">数据覆盖 {(run.quality.coverage * 100).toFixed(2)}% · {run.quality.aligned_count} / {run.quality.universe_count}</h3>
    <p className="mt-2 text-xs text-muted-foreground">质量{run.quality.passed ? '通过' : '未通过'}；缺失、排除与来源限制继续保留。</p>
    <details className="mt-3 text-xs"><summary className="cursor-pointer text-muted-foreground">排除与缺数 · {run.quality.issues.length} 项</summary><ul className="mt-2 max-h-56 space-y-2 overflow-auto">{run.quality.issues.map((issue, index) => <li key={index}><code>{issue.symbol}</code> · {issue.reason}</li>)}</ul></details>
    <details className="mt-3 text-xs"><summary className="cursor-pointer text-muted-foreground">来源与研究限制</summary><ul className="mt-2 space-y-2 leading-relaxed">{run.quality.limitations.map((text) => <li key={text}>{text}</li>)}</ul></details>
  </section>
}
