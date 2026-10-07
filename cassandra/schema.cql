-- Real-Time Crypto Market Data Pipeline: Cassandra schema
-- Run with:  docker exec -i cassandra cqlsh < cassandra/schema.cql
-- (docker compose runs it automatically through the cassandra-init service)

CREATE KEYSPACE IF NOT EXISTS crypto_pipeline
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto_pipeline;

-- Stream 1: per-coin prices from Producer 1 (topic crypto_market_prices)
-- Partition by UTC day so one query returns the newest rows of all coins;
-- newest rows first inside each day.
CREATE TABLE IF NOT EXISTS market_prices (
  capture_date          text,        -- 'YYYY-MM-DD' (UTC), partition key, added by Spark
  captured_at           timestamp,   -- when Producer 1 captured the row
  coin_id               text,
  symbol                text,
  name                  text,
  current_price         double,
  market_cap            bigint,
  market_cap_rank       int,
  total_volume          double,
  high_24h              double,
  low_24h               double,
  price_change_pct_24h  double,
  last_updated          timestamp,
  source                text,
  trend                 text,        -- UP / DOWN / FLAT, added by Spark
  volatility_pct_24h    double,      -- (high - low) / low * 100, added by Spark
  processed_at          timestamp,   -- when Spark wrote the row
  PRIMARY KEY ((capture_date), captured_at, coin_id)
) WITH CLUSTERING ORDER BY (captured_at DESC, coin_id ASC);

-- Stream 2: whole-market snapshot from Producer 2 (topic crypto_global_stats)
CREATE TABLE IF NOT EXISTS global_stats (
  capture_date              text,
  captured_at               timestamp,
  active_cryptocurrencies   int,
  markets                   int,
  total_market_cap_usd      double,
  total_volume_usd          double,
  btc_dominance_pct         double,
  eth_dominance_pct         double,
  market_cap_change_pct_24h double,
  updated_at                timestamp,
  source                    text,
  market_sentiment          text,      -- BULLISH / BEARISH / NEUTRAL, added by Spark
  processed_at              timestamp,
  PRIMARY KEY ((capture_date), captured_at)
) WITH CLUSTERING ORDER BY (captured_at DESC);
