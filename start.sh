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
"

# Start the rest of the services
docker compose up --build -d data-ingest flink-consumer


echo "All services started and initialized."

exit