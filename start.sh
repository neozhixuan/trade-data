#!/bin/bash

# Start supporting services in detached mode
docker compose up --build -d zookeeper kafka clickhouse

# Wait for Kafka and ClickHouse to be ready
echo "Waiting for Kafka and Zookeeper and Clickhouse to start..."
sleep 8

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


# Start the rest of the services
docker compose up --build -d data-ingest


echo "All services started and initialized."

exit