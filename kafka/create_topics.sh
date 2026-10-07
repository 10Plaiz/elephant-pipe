#!/bin/bash
# Creates one Kafka topic per producer so the two streams never mix.
#   crypto_market_prices  <- Producer 1 (per-coin prices)
#   crypto_global_stats   <- Producer 2 (whole-market snapshot)
set -e

BOOTSTRAP="kafka:29092"
KT="/opt/kafka/bin/kafka-topics.sh"

for TOPIC in crypto_market_prices crypto_global_stats; do
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$TOPIC" --partitions 3 --replication-factor 1
done

echo "---- Topics in the cluster ----"
$KT --bootstrap-server "$BOOTSTRAP" --list

echo "---- Topic details ----"
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic crypto_market_prices
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic crypto_global_stats
