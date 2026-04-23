# Wyckoff Funnel L2 C-lite 升级方案
## 重构决策层，复用现有指标层，优先提升 Funnel 正确性

---

## 1. 结论

推荐采用 **C-lite**：

- 重构 `Layer2` 的通过逻辑和 Track 归类逻辑。
- 复用现有 MA / RS / RPS / 吸筹 / 地量 / 暗中护盘 / SOS 等指标实现。
- 不优先考虑页面展示。
- 不把回测作为本轮前置约束。
- 以 Funnel 成功率、候选正确性、人工复核质量为第一目标。

这不是在旧六通道外面再包一层展示排序，而是把 `Layer2` 从“多个通道 OR 命中即通过”改成：

```python
metrics = build_l2_metrics(symbol)
track_scores = score_tracks(metrics, market, cfg)
selected_track = select_best_allowed_track(track_scores, market, cfg)
passed = selected_track is not None
```

---

## 2. 为什么不是方案 B，也不是纯 C

### 不选方案 B

方案 B 保留旧 L2 通道通过逻辑，只新增评分和排序。它兼容性好，但对正确性的提升有限。

当前核心问题不是“页面上没有 Top N”，而是：

- 主升、启动、左侧改善信号混在同一个 OR 通过逻辑里。
- 一个股票可能同时挂多个通道标签，但没有唯一主判断。
- 左侧观察型信号可能和右侧确认型信号拥有相似通过权重。
- 市场差异虽然已有 profile，但 L2 决策语义仍不够统一。

如果目标是 Funnel 成功率，B 太保守。

### 不选纯 C

纯 C 会从头重写 L2 指标计算。收益不高，风险更大。

现有代码已经有可复用资产：

- `FunnelConfig.for_market()` / profile overrides
- RPS 快慢排名
- RS long / short
- MA50 / MA200 / MA20
- accumulation channel
- dry volume channel
- RS divergence channel
- SOS bypass
- Markup / Accum stage detection
- layer2 rejections / explanations 基础结构

正确做法是重构“决策层”，不要重造“指标层”。

---

## 3. 本轮目标

把 L2 改为三轨决策器：

1. **Track A: 主升确认 / Momentum / Markup**
2. **Track B: 启动确认 / Early Strength / Pre-Markup**
3. **Track C: 吸筹改善 / Accumulation / Dry Volume / RS Divergence**

每只股票最终只选择一个主 Track：

```python
selected_track: "A" | "B" | "C" | None
```

允许保留旧通道命中明细作为解释字段，但它不再是主要通过依据。

---

## 4. 当前代码基线

现有 `Layer2` 并不是单轨道，而是六通道：

- 主升通道
- 潜伏通道
- 吸筹通道
- 地量蓄势
- 暗中护盘
- 点火破局

对应位置：

- `core/wyckoff_engine.py::FunnelConfig`
- `core/wyckoff_engine.py::layer2_strength_detailed`
- `core/wyckoff_engine.py::run_funnel`

本方案应修改文档和实现口径：

旧表述：

> 当前 Layer2 是单轨道、强 AND 过滤。

改为：

> 当前 Layer2 已有多个信号通道，但通过逻辑仍偏硬触发和 OR 聚合，缺少统一 Track 主判断、评分阈值和市场分轨决策。

---

## 5. 设计原则

### 5.1 指标层复用

先抽出统一指标快照，不重写所有算法：

```python
@dataclass
class L2Metrics:
    symbol: str
    market: str

    close: float | None
    ma20: float | None
    ma50: float | None
    ma200: float | None
    bias_200: float | None

    rs_long: float | None
    rs_short: float | None
    rps_fast: float | None
    rps_slow: float | None
    rps_slope: float | None

    ret_5: float | None
    ret_10: float | None
    ret_20: float | None
    breakout_proximity_20: float | None
    breakout_proximity_60: float | None
    volume_expansion: float | None

    price_from_250d_low: float | None
    range_60_pct: float | None
    dry_volume_ratio: float | None
    ma_gap_pct: float | None

    old_channels: dict[str, bool]
```

### 5.2 决策层重构

三轨评分函数只依赖 `L2Metrics`：

```python
score_track_a(metrics, cfg, market) -> TrackScoreDetail
score_track_b(metrics, cfg, market) -> TrackScoreDetail
score_track_c(metrics, cfg, market) -> TrackScoreDetail
select_l2_track(details, cfg, market) -> L2Decision
```

### 5.3 L4 不前移

Spring / LPS / EVR / SOS 是确认层。

- SOS 可以作为 Track A/B 的加分或 bypass 证据。
- Spring / LPS 不作为 L2 必要条件。
- L2 的职责是识别结构候选，不负责最终买点确认。

---

## 6. 市场允许规则

```python
MARKET_ALLOWED_TRACKS = {
    "cn": ("A", "B", "C"),
    "us": ("A", "B"),
    "hk": ("B", "C", "A"),
}
```

### A股

- A / B / C 全开。
- 但 C 轨必须保持“改善”而不是纯低位。
- C 轨通过后仍应依赖 L4 做买点确认。

### 美股

- 只开 A / B。
- C 默认关闭。
- 更重视 RPS、RS、RPS slope、趋势结构。

### 港股

- 主开 B / C。
- A 只允许高流动性、强趋势、结构干净的少数标的。
- 增加过度拉升和流动性不稳定惩罚。

---

## 7. Track 必要条件

### Track A: 主升确认

必要条件：

```python
ma50 > ma200
close >= ma50
rps_fast >= track_a_rps_fast_min
rps_slow >= track_a_rps_slow_min
rs_long >= track_a_rs_long_min
rps_slope >= track_a_rps_slope_min
bias_200 <= track_a_bias_200_max
```

默认建议：

- CN: `rps_fast >= 75`, `rps_slow >= 70`
- US: `rps_fast >= 85`, `rps_slow >= 80`
- HK: `rps_fast >= 80`, `rps_slow >= 72`, 并提高流动性要求

### Track B: 启动确认

必要条件：

```python
rps_fast >= track_b_rps_fast_min
rs_short > track_b_rs_short_min
rps_slope > 0
close >= ma20 or close >= ma50
breakout_proximity_60 >= track_b_breakout_proximity_min
```

Track B 不等同于旧“潜伏通道”。

旧潜伏通道的长强短弱可以作为 Track B 的一种证据，但 Track B 的主语义是“已经开始转强并接近突破”。

### Track C: 吸筹改善

必要条件：

```python
price_from_250d_low <= track_c_price_from_low_max
old_channels["accum"] or old_channels["dry_vol"] or old_channels["rs_div"]
not severe_downtrend
```

注意：

- `ma_gap_pct` 不应作为所有 C 轨的硬必要条件。
- 对 `accum` 可以要求均线胶着。
- 对 `dry_vol` 和 `rs_div` 应允许更宽的 MA 状态，但要加入下跌中继保护。

---

## 8. 评分模型

所有分数归一到 0-100。

### 通用子分

```python
trend_maturity_score =
    0.30 * ma_alignment_score
  + 0.25 * rps_fast_score
  + 0.20 * rps_slow_score
  + 0.15 * rs_long_score
  + 0.10 * rs_short_score
```

```python
early_strength_score =
    0.30 * rs_short_score
  + 0.25 * rps_slope_score
  + 0.20 * breakout_proximity_score
  + 0.15 * recent_return_score
  + 0.10 * volume_expansion_score
```

```python
accum_readiness_score =
    0.30 * accumulation_channel_score
  + 0.25 * dry_volume_score
  + 0.20 * rs_divergence_score
  + 0.15 * low_position_score
  + 0.10 * ma_convergence_score
```

### Track A

```python
track_a_score =
    0.55 * trend_maturity_score
  + 0.25 * early_strength_score
  + 0.10 * markup_score
  + 0.10 * sos_score
  - overextended_penalty
```

### Track B

```python
track_b_score =
    0.35 * trend_maturity_score
  + 0.45 * early_strength_score
  + 0.10 * breakout_readiness_score
  + 0.10 * volume_confirmation_score
  - failed_breakout_penalty
```

### Track C

```python
track_c_score =
    0.15 * trend_maturity_score
  + 0.20 * early_strength_score
  + 0.55 * accum_readiness_score
  + 0.10 * support_resilience_score
  - downtrend_continuation_penalty
```

---

## 9. L2 通过规则

```python
def select_l2_track(track_details, market, cfg):
    allowed = MARKET_ALLOWED_TRACKS.get(market, ("A", "B", "C"))

    valid = [
        d for d in track_details
        if d.track in allowed
        and d.required_passed
        and d.score >= cfg.track_min_score[d.track]
    ]

    if not valid:
        return None

    return max(valid, key=lambda d: d.score)
```

建议默认阈值：

```python
track_a_min_score = 70
track_b_min_score = 62
track_c_min_score = 58
```

市场差异：

- US: A/B 阈值上调，C 禁用。
- HK: A 阈值上调，B/C 保持中等阈值，流动性 penalty 加重。
- CN: 三轨全开，C 阈值不宜太低。

---

## 10. Top N 的处理

页面展示不重要时，Top N 不应成为核心目标。

本轮建议：

- L2 输出所有通过三轨决策的股票。
- 同时输出 `layer2_ranked_symbols` 供后续 AI 或人工复核优先使用。
- 暂不强制用 Top N 截断进入 L3/L4，除非候选数过大。

如果需要截断，优先用总分截断，而不是固定轨道配额。

---

## 11. 数据结构

```python
@dataclass(frozen=True)
class TrackScoreDetail:
    track: str
    required_passed: bool
    score: float
    reasons: dict[str, object]
    penalties: dict[str, float]
```

```python
@dataclass(frozen=True)
class L2Decision:
    symbol: str
    passed: bool
    selected_track: str | None
    selected_score: float
    track_scores: dict[str, float]
    old_channels: dict[str, bool]
    reasons: dict[str, object]
```

`FunnelResult` 可新增字段，但不是本轮正确性的核心：

- `layer2_decisions`
- `layer2_track_map`
- `layer2_score_map`
- `layer2_ranked_symbols`

如果担心 NamedTuple 兼容性，可以先把这些内容放进 `explanations[sym]["layer2_decision"]`。

---

## 12. 实施步骤

### Phase 1: 抽 L2Metrics

- 从 `layer2_strength_detailed` 中抽出 RS/RPS/MA/低位/缩量/突破接近等指标。
- 保留旧通道布尔值作为 `old_channels`。
- 不改变主流程输出。

### Phase 2: 实现三轨评分

- 新增 `score_track_a`
- 新增 `score_track_b`
- 新增 `score_track_c`
- 单测覆盖典型 A/B/C 样本和不通过样本。

### Phase 3: 切换 L2 通过逻辑

- `layer2_strength_detailed` 内部改用三轨决策。
- `channel_map` 临时映射为：
  - Track A -> `主升确认`
  - Track B -> `启动确认`
  - Track C -> `吸筹改善`
- 旧通道放入 explanations，作为辅助说明。

### Phase 4: 市场 profile 接入

- 在 `FunnelConfig` 增加 track 阈值和开关。
- `cn` 开 A/B/C。
- `us` 只开 A/B。
- `hk` 开 B/C，A 高门槛。

### Phase 5: 人工复核校准

不用先做页面，也不用先做回测。

每个市场抽样复核：

- 通过样本 30 个。
- 拒绝样本 30 个。
- 重点看 Track 归类是否符合图形结构。
- 调整阈值和 penalty。

---

## 13. 正确性验收标准

本轮验收不以候选数量最大化为目标。

必须满足：

1. 美股不会输出明显左侧吸筹 C 轨标的。
2. 港股 A 轨只出现在高流动性、强趋势、结构干净标的。
3. C 轨不再因为“低位”本身通过，必须有吸筹、地量或拒绝新低证据。
4. B 轨必须体现短期转强和突破接近，不能只是长期强、短期弱。
5. 每个通过 L2 的股票都有唯一 `selected_track`。
6. 每个拒绝样本能解释是必要条件失败，还是评分不足。

---

## 14. 最小上线规则

如果要先快速落地，使用下面的 C-lite MVP：

```python
track_a_required =
    ma50 > ma200
    and close >= ma50
    and rps_fast >= market_a_fast_min
    and rps_slow >= market_a_slow_min
    and rs_long >= market_a_rs_long_min
    and rps_slope >= market_a_slope_min
```

```python
track_b_required =
    rps_fast >= market_b_fast_min
    and rs_short > 0
    and rps_slope > 0
    and (close >= ma20 or close >= ma50)
    and breakout_proximity_60 >= market_b_breakout_min
```

```python
track_c_required =
    price_from_250d_low <= market_c_low_max
    and (accum_ok or dry_vol_ok or rs_div_ok)
    and not severe_downtrend
```

然后：

- CN: A / B / C 全算。
- US: A / B。
- HK: B / C，A 仅高门槛开放。

---

## 15. 一句话总结

**C-lite 的核心是：不再让旧六通道 OR 决定 L2 通过，而是复用旧指标，重构成市场允许集 + 三轨评分 + 唯一主 Track 决策。**
