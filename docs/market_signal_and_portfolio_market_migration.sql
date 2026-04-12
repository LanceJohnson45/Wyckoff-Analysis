-- Market-aware support for market_signal_daily + portfolio_positions
-- Run this in Supabase SQL editor before enabling US writes / UI editing.

begin;

-- 1) market_signal_daily: split CN / US rows by (trade_date, market)
alter table if exists public.market_signal_daily
  add column if not exists market text;

update public.market_signal_daily
set market = coalesce(nullif(lower(trim(market)), ''), 'cn')
where market is null or trim(market) = '';

alter table if exists public.market_signal_daily
  alter column market set default 'cn';

alter table if exists public.market_signal_daily
  alter column market set not null;

create index if not exists idx_market_signal_daily_market_trade_date
  on public.market_signal_daily (market, trade_date desc);

create unique index if not exists uq_market_signal_daily_trade_date_market
  on public.market_signal_daily (trade_date, market);

-- 2) portfolio_positions: preserve symbol namespace for CN + US holdings
alter table if exists public.portfolio_positions
  add column if not exists market text;

update public.portfolio_positions
set market = coalesce(nullif(lower(trim(market)), ''), 'cn')
where market is null or trim(market) = '';

alter table if exists public.portfolio_positions
  alter column market set default 'cn';

alter table if exists public.portfolio_positions
  alter column market set not null;

create index if not exists idx_portfolio_positions_portfolio_market
  on public.portfolio_positions (portfolio_id, market);

drop index if exists uq_portfolio_positions_portfolio_code;
create unique index if not exists uq_portfolio_positions_portfolio_market_code
  on public.portfolio_positions (portfolio_id, market, code);

commit;

-- Notes:
-- 1) Existing CN rows are backfilled to market='cn'.
-- 2) If your project has RLS policies or views referencing only (portfolio_id, code)
--    or only trade_date, review them and widen predicates to include market where needed.
-- 3) App code keeps CN fallback compatibility for market_signal_daily reads, but US writes
--    require the new market column + unique index to be safe.
