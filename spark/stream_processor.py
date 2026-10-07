"""
Spark Structured Streaming job
  Kafka topic crypto_market_prices  -> process -> Cassandra crypto_pipeline.market_prices
  Kafka topic crypto_global_stats   -> process -> Cassandra crypto_pipeline.global_stats

Submitted by docker compose (service `spark`). Spark UI: http://localhost:4040
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "crypto_pipeline"
CHECKPOINT_ROOT = "/tmp/checkpoints"
TRIGGER = "10 seconds"

# ------------------------------------------------------------------ message schemas (match the producers' JSON)
MARKET_SCHEMA = StructType([
    StructField("coin_id", StringType()),
    StructField("symbol", StringType()),
    StructField("name", StringType()),
    StructField("current_price", DoubleType()),
    StructField("market_cap", DoubleType()),
    StructField("market_cap_rank", IntegerType()),
    StructField("total_volume", DoubleType()),
    StructField("high_24h", DoubleType()),
    StructField("low_24h", DoubleType()),
    StructField("price_change_pct_24h", DoubleType()),
    StructField("last_updated", StringType()),
    StructField("captured_at", StringType()),
    StructField("source", StringType()),
])

GLOBAL_SCHEMA = StructType([
    StructField("active_cryptocurrencies", IntegerType()),
    StructField("markets", IntegerType()),
    StructField("total_market_cap_usd", DoubleType()),
    StructField("total_volume_usd", DoubleType()),
    StructField("btc_dominance_pct", DoubleType()),
    StructField("eth_dominance_pct", DoubleType()),
    StructField("market_cap_change_pct_24h", DoubleType()),
    StructField("updated_at", StringType()),
    StructField("captured_at", StringType()),
    StructField("source", StringType()),
])


def parse_json(df: DataFrame, schema: StructType) -> DataFrame:
    """Kafka rows (binary `value`) -> one column per JSON field."""
    return (df.select(F.col("value").cast("string").alias("json"))
              .select(F.from_json("json", schema).alias("d"))
              .select("d.*"))


def transform_market(df: DataFrame) -> DataFrame:
    """Clean, type and enrich the per-coin stream."""
    return (
        df.filter(F.col("coin_id").isNotNull() & F.col("current_price").isNotNull())
          .withColumn("captured_at", F.col("captured_at").cast("timestamp"))
          .withColumn("last_updated", F.col("last_updated").cast("timestamp"))
          .withColumn("capture_date", F.date_format("captured_at", "yyyy-MM-dd"))
          .withColumn("market_cap", F.col("market_cap").cast("long"))
          .withColumn("price_change_pct_24h", F.round("price_change_pct_24h", 4))
          .withColumn("trend",
                      F.when(F.col("price_change_pct_24h") > 0, "UP")
                       .when(F.col("price_change_pct_24h") < 0, "DOWN")
                       .otherwise("FLAT"))
          .withColumn("volatility_pct_24h",
                      F.when(F.col("low_24h") > 0,
                             F.round((F.col("high_24h") - F.col("low_24h")) / F.col("low_24h") * 100, 4)))
          .withColumn("processed_at", F.current_timestamp())
    )


def transform_global(df: DataFrame) -> DataFrame:
    """Clean, type and enrich the whole-market stream."""
    return (
        df.filter(F.col("total_market_cap_usd").isNotNull())
          .withColumn("captured_at", F.col("captured_at").cast("timestamp"))
          .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
          .withColumn("capture_date", F.date_format("captured_at", "yyyy-MM-dd"))
          .withColumn("btc_dominance_pct", F.round("btc_dominance_pct", 4))
          .withColumn("eth_dominance_pct", F.round("eth_dominance_pct", 4))
          .withColumn("market_sentiment",
                      F.when(F.col("market_cap_change_pct_24h") >= 1, "BULLISH")
                       .when(F.col("market_cap_change_pct_24h") <= -1, "BEARISH")
                       .otherwise("NEUTRAL"))
          .withColumn("processed_at", F.current_timestamp())
    )


def cassandra_writer(table: str, preview_cols: list):
    """foreachBatch sink: prints proof of each micro-batch, then appends it to Cassandra."""
    def write_batch(batch_df: DataFrame, batch_id: int):
        batch_df.persist()
        count = batch_df.count()
        print(f"\n===== [{table}] micro-batch {batch_id}: {count} rows =====", flush=True)
        if count > 0:
            batch_df.select(*preview_cols).show(5, truncate=False)
            (batch_df.write
             .format("org.apache.spark.sql.cassandra")
             .mode("append")
             .options(keyspace=KEYSPACE, table=table)
             .save())
            print(f"===== [{table}] wrote {count} rows to Cassandra {KEYSPACE}.{table} =====", flush=True)
        batch_df.unpersist()
    return write_batch


def main():
    spark = (
        SparkSession.builder
        .appName("CryptoPipeline-Kafka-Spark-Cassandra")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    def read_topic(topic: str):
        return (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load())

    market = transform_market(parse_json(read_topic("crypto_market_prices"), MARKET_SCHEMA))
    global_stats = transform_global(parse_json(read_topic("crypto_global_stats"), GLOBAL_SCHEMA))

    (market.writeStream
        .queryName("market_prices_to_cassandra")
        .foreachBatch(cassandra_writer("market_prices",
                      ["captured_at", "symbol", "current_price", "price_change_pct_24h", "trend"]))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/market_prices")
        .trigger(processingTime=TRIGGER)
        .start())

    (global_stats.writeStream
        .queryName("global_stats_to_cassandra")
        .foreachBatch(cassandra_writer("global_stats",
                      ["captured_at", "total_market_cap_usd", "btc_dominance_pct", "market_sentiment"]))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/global_stats")
        .trigger(processingTime=TRIGGER)
        .start())

    print("Spark streaming started: 2 queries running. Spark UI -> http://localhost:4040", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
