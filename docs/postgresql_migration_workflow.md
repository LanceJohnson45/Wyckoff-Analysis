# Supabase -> PostgreSQL 迁移工作流（自用分析版）

## 目标范围

本迁移只覆盖当前“自用 + 每日数据分析”需要的表：

- `stock_hist_cache`
- `market_signal_daily`
- `recommendation_tracking`
- `signal_pending`
- `index_constituents_snapshot`
- `portfolios`
- `portfolio_positions`
- `trade_orders`
- `daily_nav`

不包含：

- `user_settings`
- `job_usage`
- `user_tier`
- Supabase Auth / RLS 相关能力

建表 SQL 在：

- [docs/postgresql_analysis_schema.sql](/Volumes/E/github/Wyckoff-Analysis/docs/postgresql_analysis_schema.sql)

## 推荐迁移顺序

1. 在自建 PostgreSQL 上执行建表 SQL
2. 从 Supabase 导出 JSONL
3. 导入 PostgreSQL
4. 验证行数 / 抽样对账
5. 再开始切代码读写路径

## 导出方案 A：走仓库脚本

适合当前 Supabase PostgREST 还可访问的情况。

```bash
python3 scripts/export_supabase_analysis_tables.py \
  --output-dir data/pg_migration_export
```

可选只导某一张表：

```bash
python3 scripts/export_supabase_analysis_tables.py \
  --table stock_hist_cache \
  --output-dir data/pg_migration_export
```

输出：

- `data/pg_migration_export/<table>.jsonl`
- `data/pg_migration_export/_manifest.json`

## 导入方案 A：走仓库脚本

```bash
python3 scripts/import_postgres_analysis_tables.py \
  --dsn 'postgresql://USER:PASSWORD@HOST:5432/DBNAME' \
  --input-dir data/pg_migration_export
```

可选只导入某一张表：

```bash
python3 scripts/import_postgres_analysis_tables.py \
  --dsn 'postgresql://USER:PASSWORD@HOST:5432/DBNAME' \
  --table stock_hist_cache \
  --input-dir data/pg_migration_export
```

## 导出方案 B：Supabase 流量太紧时

如果不想继续走 PostgREST 导出，可以直接从 Supabase 的 PostgreSQL 层导出。

优先方式：

1. 在 Supabase 后台拿数据库连接串
2. 本地用 `pg_dump` 或 `psql` 的 `\copy`
3. 导出为 CSV 或 SQL

示例思路：

```bash
pg_dump 'postgresql://...' \
  --data-only \
  --table=public.stock_hist_cache \
  --table=public.market_signal_daily \
  --table=public.recommendation_tracking \
  --table=public.signal_pending
```

或按表导 CSV：

```bash
psql 'postgresql://...' \
  -c "\copy public.stock_hist_cache to 'stock_hist_cache.csv' csv header"
```

这样走的是数据库连接，不是项目里那层 Supabase Python/PostgREST 读流量。

## 对账建议

迁移后至少做这几步：

1. 对比每张表总行数
2. 对 `stock_hist_cache` 抽样几只股票，核对 `min(date) / max(date) / count(*)`
3. 对 `market_signal_daily` 检查 `(trade_date, market)` 唯一键
4. 对 `recommendation_tracking` 检查 `(market, symbol, recommend_date)` 唯一键
5. 对 `portfolio_positions` 检查 `(portfolio_id, market, code)` 唯一键

## 代码切换顺序

推荐先切：

1. `stock_hist_cache`
2. `market_signal_daily`
3. `recommendation_tracking`
4. `signal_pending`

如果你还保留 Step4 / 持仓管理，再切：

5. `portfolios`
6. `portfolio_positions`
7. `trade_orders`
8. `daily_nav`
