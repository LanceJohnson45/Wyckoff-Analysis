import { z } from 'zod'
import { researchDay as day, researchHash as hash, researchId as id } from './contract-values'

export const anchorChartSchema = z.object({
  run_id: id, report_id: id, snapshot_id: id, snapshot_hash: hash, as_of: day, target_session: day,
  company: z.object({ symbol: z.string(), name: z.string().min(1) }), calculation_available: z.boolean(),
  anchors: z.array(z.object({ index: z.number().int().nonnegative(), kind: z.enum(['swing_low', 'swing_high', 'breakout']), date: day, confirmed_date: day, daily_estimate: z.number().finite().nullable(), hard_filter_enabled: z.literal(false), ref_prefix: z.string() })),
  selected_index: z.number().int().nonnegative().nullable(),
  points: z.array(z.object({ session_date: day, value: z.number().finite().nullable() })).max(320), limitations: z.array(z.string()),
}).superRefine((chart, context) => {
  const invalid = (message: string) => context.addIssue({ code: z.ZodIssueCode.custom, message })
  if (chart.anchors.some((row, index) => row.index !== index || row.date > row.confirmed_date || row.confirmed_date > chart.as_of || row.ref_prefix !== `martin/${chart.company.symbol}/anchors/${index}`)) invalid('原锚点顺序、确认日期或引用无效')
  if ((!chart.calculation_available && chart.anchors.length > 0) || (chart.selected_index === null && (chart.anchors.length > 0 || chart.points.length > 0))) invalid('锚点与计算状态不一致')
  if (chart.selected_index !== null) {
    const selected = chart.anchors[chart.selected_index]
    if (!selected) invalid('所选锚点不存在')
    else if (chart.points.some((row, index) => row.session_date < selected.confirmed_date || row.session_date > chart.as_of || (index > 0 && row.session_date <= chart.points[index - 1]!.session_date))) invalid('曲线超出确认日或顺序异常')
  }
})
export type ResearchAnchorChart = z.infer<typeof anchorChartSchema>
export const anchorLabels = { swing_low: '摆动低点', swing_high: '摆动高点', breakout: '突破' }
