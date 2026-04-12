-- recommendation_tracking: add explicit market/symbol support for CN + US
-- Run this in Supabase SQL editor before enabling US recommendation tracking writes.

alter table if exists public.recommendation_tracking
  add column if not exists market text;

alter table if exists public.recommendation_tracking
  add column if not exists symbol text;

update public.recommendation_tracking
set market = coalesce(nullif(lower(trim(market)), ''), 'cn')
where market is null or trim(market) = '';

update public.recommendation_tracking
set symbol = lpad(code::text, 6, '0')
where (symbol is null or trim(symbol) = '')
  and code is not null;

alter table if exists public.recommendation_tracking
  alter column market set default 'cn';

alter table if exists public.recommendation_tracking
  alter column market set not null;

alter table if exists public.recommendation_tracking
  alter column symbol set not null;

create index if not exists idx_recommendation_tracking_market_symbol
  on public.recommendation_tracking (market, symbol);

create unique index if not exists uq_recommendation_tracking_market_symbol_date
  on public.recommendation_tracking (market, symbol, recommend_date);
