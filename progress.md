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
- Tightened US maintenance correctness: switched S&P500 source fetch to the MediaWiki API, fixed multi-ticker yfinance parsing, and made cache writes verifiable instead of counting prepared rows as success.
- Added HK market pipeline support for scheduled jobs: `hk` market surface, HSI+HSTECH constituent snapshot parsing, HK bootstrap/monthly-sync/daily-refresh workflows, and HK funnel prewarm/funnel workflows.

## 2026-04-23
- Started Phase 6 yfinance enhancement: market-cap by shares cache, candidate-level fundamental factors, and news context for AI analysis.
- Added `integrations/yfinance_enrichment.py` with shares cache, market-cap-from-close calculation, candidate fundamental factors, and news summaries.
- Wired shares-based market cap into `scripts/wyckoff_funnel.py` before L1, and made L1 tolerate partial market-cap caches.
- Wired Step3 candidate enrichment into AI payloads and Step4 candidate metadata.
- Added targeted tests for yfinance enrichment and partial market-cap cache behavior.
- Started Phase 7 FunnelConfig template split; reviewed A/HK/US strategy notes and mapped them into market/style template boundaries.
- Implemented market/style template composition in `FunnelConfig`, added named profiles and env selection via `FUNNEL_CONFIG_PROFILE` / `FUNNEL_PROFILE`.
- Added profile metadata to funnel metrics and benchmark tuning context.
- Verified `tests/test_wyckoff_engine.py` with `.venv` pytest: 14 passed.
- Simplified public profile surface to only `cn`, `hk`, and `us`; legacy `*_value` names now resolve to these canonical profiles.
