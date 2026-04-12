# Findings

## Confirmed entrypoints
- Web background dispatch: `app/background_jobs.py` -> `integrations/github_actions.py` -> `.github/workflows/web_quant_jobs.yml` -> `scripts/web_background_job.py`.
- Funnel scheduling: `.github/workflows/wyckoff_funnel.yml` -> `scripts/daily_job.py` / `scripts/wyckoff_funnel.py`.

## Confirmed lower-layer support
- `integrations/stock_hist_repository.py` supports `market='cn'|'us'`.
- `integrations/data_source.py` routes US equity history to yfinance.
- `integrations/fetch_a_share_csv.py` contains US window/symbol helpers.

## Confirmed correctness gaps
- `scripts/step3_batch_report.py` still used CN trading window and CN `_fetch_hist()` path.
- `pages/AIAnalysis.py` manual parsing and latest-result loading were not market-aware.
- `pages/WyckoffScreeners.py` stored session candidates without preserving benchmark context/market metadata for the AI page.
- `integrations/github_actions.py` latest-result replay filtered only by job kind and user, not by market.

## Deferred high-risk areas
- `integrations/supabase_recommendation.py` stores CN-shaped numeric codes and requires schema/model redesign.
- `scripts/backtest_runner.py` still assumes CN normalization and CN benchmark.

## Step4 / premarket market-aware findings
- `scripts/step4_rebalancer.py` is the main Step4 consumer and already merges `benchmark_regime` + `premarket_regime`, but current trade-date / same-day logic is likely CN-clock biased.
- `scripts/premarket_risk_job.py` already has some US-related helpers (`_latest_expected_us_trade_date`) but the overall semantics remain CN-oriented (`A50 + VIX`, Asia/Shanghai scheduling, Beijing-day write key).
- `integrations/supabase_market_signal.py` is the critical storage seam; if signals are keyed only by `trade_date`, CN and US semantics can overwrite or ambiguously share one row.
- `scripts/daily_job.py` and `pages/Portfolio.py` are likely secondary consumers/orchestrators that need auditing for market-aware reads rather than CN-default "latest day" assumptions.
