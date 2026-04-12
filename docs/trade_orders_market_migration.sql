-- Market-aware support for trade_orders
-- Run this in Supabase SQL editor before enabling mixed-market AI order writes.

begin;

-- 1) trade_orders: persist CN / US order rows explicitly by market
alter table if exists public.trade_orders
  add column if not exists market text;

update public.trade_orders
set market = coalesce(nullif(lower(trim(market)), ''), 'cn')
where market is null or trim(market) = '';

alter table if exists public.trade_orders
  alter column market set default 'cn';

alter table if exists public.trade_orders
  alter column market set not null;

create index if not exists idx_trade_orders_portfolio_trade_date_market
  on public.trade_orders (portfolio_id, trade_date desc, market);

commit;

-- Notes:
-- 1) Current trade_orders is empty, so the backfill UPDATE is effectively a no-op for now.
-- 2) App code should write explicit market values for mixed CN / US orders; DEFAULT 'cn'
--    remains only as a backward-compatible fallback.
-- 3) daily_nav intentionally stays unchanged because it is still modeled as a
--    portfolio-level daily snapshot, not a per-market NAV table.
