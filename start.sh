#!/bin/bash

# Build the flink java project
cd flink-binance-consumer

 # This cleans the target direcory, compiles the project, and packages it into a JAR file
 # - this is necessary as we need to build a Fat JAR file that includes all dependencies
mvn clean compile package
if [ $? -ne 0 ]; then # Check if the previous command was successful TODO
  echo "Maven build failed. Exiting."
  exit 1
fi
cd ..

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

CREATE TABLE IF NOT EXISTS trades.kline_events (
    event_type String,        -- e
    event_time DateTime,      -- E
    symbol String             -- s
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);

CREATE TABLE IF NOT EXISTS trades.kline_data (
    symbol String,                -- k.s
    interval String,              -- k.i
    open_time DateTime,           -- k.t
    close_time DateTime,          -- k.T
    first_trade_id UInt64,        -- k.f
    last_trade_id UInt64,         -- k.L
    open Float64,                 -- k.o
    close Float64,                -- k.c
    high Float64,                 -- k.h
    low Float64,                  -- k.l
    volume Float64,               -- k.v
    trade_count UInt32,           -- k.n
    is_closed UInt8,              -- k.x
    quote_asset_volume Float64,   -- k.q
    taker_buy_base_volume Float64,  -- k.V
    taker_buy_quote_volume Float64, -- k.Q
    event_time DateTime           -- redundant but useful for joining back
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(open_time)
ORDER BY (symbol, interval, open_time);

CREATE TABLE trades.symbols (
    symbol String PRIMARY KEY,
    base_asset String,
    quote_asset String,
    asset_class String, -- e.g., crypto, stablecoin
    launched Date
) ENGINE = MergeTree()
ORDER BY symbol;
"

# Start the rest of the services
docker compose up --build -d data-ingest flink-consumer


echo "All services started and initialized."

exit