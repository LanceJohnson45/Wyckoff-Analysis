# Progress Log

## 2026-04-11
- Reviewed current CN/US support and identified that lower-layer history fetch is market-aware but upper layers still contain CN defaults.
- Defined phased rollout plan.
- Started Phase 1 implementation focused on Web/background market propagation.
- Started Phase 5 investigation for Step4 market-aware risk-control and US premarket semantics; collected plan-agent guidance and targeted codebase exploration results.

## 2026-04-13
- Added a reusable US S&P500 constituent helper (`integrations/us_sp500_universe.py`) with snapshot loading/saving and Wikipedia-based cold-start fetching.
- Added `scripts/us_sp500_maintenance.py` for four US jobs: monthly constituent sync, full bootstrap, daily short-window refresh, and funnel prewarm support.
- Added `scripts/funnel_prewarm.py` so scheduled funnel workflows can warm cache windows before running the main funnel job.
- Updated US funnel to support `FUNNEL_POOL_MODE=sp500` and switched US benchmark fetches to `SPY` / `IWM` instead of CN indexes.
- Added new workflows for US bootstrap, monthly constituent sync, and daily bar refresh; updated CN/US funnel workflows to run a prewarm step.
- Added GitHub Actions cache restore/save wiring for `data/us_sp500_constituents.json` in all US maintenance/funnel workflows, so fresh runners can recover the latest known constituent snapshot before falling back to a live fetch.
- Added workflow-level env knobs for US batch size / sleep / history windows and documented the full US S&P500 pipeline plus tuning guidance in `README.md` and `docs/DEPLOYMENT.md`.
