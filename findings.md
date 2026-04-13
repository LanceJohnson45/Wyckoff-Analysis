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

## 2026-04-13 US pipeline findings
- `stock_hist_cache` already gives idempotent upsert semantics for `US:{symbol}` keys, so the new US bootstrap/refresh path can reuse existing storage without a second cache layer.
- The previous US funnel path depended on `FUNNEL_POOL_MANUAL_SYMBOLS`; this was replaced with an `sp500` pool mode resolved from a maintained constituent snapshot or a fresh fetch.
- `integrations/data_source.fetch_index_hist()` only supported CN indexes before this session; US benchmark support required a yfinance-backed index path so SPY/IWM can drive US funnel and step3 relative-strength context.
- A durable cross-run S&P500 diff store does not exist in the current repo. The implemented scripts still behave correctly on cold runners because daily refresh/funnel fetch the current constituent set directly, but the monthly added/removed diff is most informative when the snapshot file persists between runs.
