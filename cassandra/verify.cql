-- Proof queries for Part 5. Open cqlsh with:  docker exec -it cassandra cqlsh
-- Replace 2026-10-07 with today's UTC date.

DESCRIBE KEYSPACE crypto_pipeline;

USE crypto_pipeline;

-- newest 20 coin rows stored by Spark
SELECT captured_at, symbol, current_price, price_change_pct_24h, trend, processed_at
FROM market_prices WHERE capture_date = '2026-10-07' LIMIT 20;

-- newest 10 whole-market snapshots
SELECT captured_at, total_market_cap_usd, btc_dominance_pct, market_cap_change_pct_24h, market_sentiment
FROM global_stats WHERE capture_date = '2026-10-07' LIMIT 10;

-- row counts (run twice, a minute apart, to show the tables are growing)
SELECT COUNT(*) FROM market_prices WHERE capture_date = '2026-10-07';
SELECT COUNT(*) FROM global_stats WHERE capture_date = '2026-10-07';
