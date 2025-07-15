#!/bin/bash

# Start all services in detached mode
docker compose up -d

# Wait for Kafka and ClickHouse to be ready
echo "Waiting for Kafka and ClickHouse to start..."
sleep 10

# Create Kafka topic (if not exists)
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic binance.kline \
  --partitions 3 \
  --replication-factor 1

# Create ClickHouse table (if not exists)
docker compose exec clickhouse clickhouse-client --password default --query "
CREATE DATABASE IF NOT EXISTS trades;
CREATE TABLE IF NOT EXISTS trades.kline_agg (
    symbol String,
    window_start DateTime,
    avg_close Float64
) ENGINE = MergeTree()
ORDER BY (symbol, window_start);
"

echo "All services started and initialized."

exit