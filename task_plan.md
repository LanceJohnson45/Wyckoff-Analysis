# CN/US Support Rollout Plan

## Goal
Upgrade the repository from partial US-stock support to end-to-end CN/US support while preserving existing CN behavior.

## Phases

### Phase 1 - Market propagation for Web/background paths [in_progress]
- Make Web background funnel and background AI report flows explicitly market-aware.
- Ensure latest-result replay in Streamlit does not mix CN and US results.
- Ensure `market` survives from page input -> workflow dispatch -> background execution -> result payload -> UI replay.
- Keep benchmark/risk logic temporarily A-share-style for US where needed.

### Phase 2 - Scheduled jobs and GitHub Actions US variants [pending]
- Add US workflow(s) or matrix jobs for scheduled funnel/background runs.
- Separate CN/US concurrency groups, artifacts, and env wiring.

### Phase 3 - Core business-chain marketization [pending]
- Recommendation tracking storage and lookups become market-aware.
- Backtest and portfolio/risk flows stop assuming 6-digit CN codes and fixed CN benchmark/calendar.

### Phase 4 - US-native benchmark/risk optimization [pending]
- Replace temporary CN-style benchmark/risk reuse with US-specific benchmarks and calendars.

### Phase 5 - Step4 market-aware risk-control [in_progress]
- Audit `scripts/step4_rebalancer.py`, `scripts/premarket_risk_job.py`, `integrations/supabase_market_signal.py`, and scheduling/UI readers for CN-only assumptions.
- Introduce market-aware market-signal read/write semantics so CN and US risk signals do not silently share one trade-date identity.
- Make Step4 resolve trade date / same-day logic / risk banner by market.
- Split or parameterize premarket risk evaluation so US semantics no longer depend on CN A50 assumptions.
- Run targeted validation and summarize residual risk, especially any remaining CN-centric UI or workflow assumptions.

## Phase 1 First Slice
1. Persist planning files.
2. Patch `scripts/wyckoff_funnel.py` to include `market` in `symbols_for_report`.
3. Patch `pages/WyckoffScreeners.py` to store session market + benchmark context and load market-specific latest results.
4. Patch `pages/AIAnalysis.py` to support CN/US batch market selection, market-aware manual parsing, and market-specific latest-result loading.
5. Patch `app/background_jobs.py` + `integrations/github_actions.py` to filter latest workflow results by market.
6. Patch `scripts/web_background_job.py` + `scripts/step3_batch_report.py` so background AI reports actually use the selected market for data fetching.

## Deferred from Phase 1
- Supabase recommendation schema migration.
- Backtest symbol/benchmark normalization.
- Single-stock local page full US support.
- Scheduled workflow duplication for US.
