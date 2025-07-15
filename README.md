# OLAP Pipeline for Bitcoin Kline Data

This project outlines a test pipeline to read live trade data from Binance WebSocket into a Kafka MQ, and process this stream of data using Apache Flink, storing 1 minute windowed aggregated data into ClickHouse, a columnar database.

<img width="1241" height="682" alt="image" src="https://github.com/user-attachments/assets/0bc9b335-39b8-45c3-af51-4b6ea7a24009" />

Flink Windowed Aggregation

<img width="887" height="563" alt="image" src="https://github.com/user-attachments/assets/fa94940f-92de-4488-98c5-b82b56d21ec2" />


## Setup

Java 11 support

```sh
export JAVA_HOME=/c/'Program Files'/java/jdk-11.0.13/ && export PATH=$JAVA_HOME/bin:$PATH

java -version  # should now show Java 11
```

Start docker services for Zookeeper and Kafka

```sh
chmod +x ./start.sh

bash start.sh
```

Start the Binance WebSocket client that will ingest the data into a Kafka broker.

```sh
cd data-ingest
go run .
cd ..
```

Expected logs:

```sh
2025/07/13 17:01:28 websocketClient.go:26: Connecting to Binance WebSocket at wss://stream.binance.com:9443/ws/bnbbtc@trade
2025/07/13 17:01:28 websocketClient.go:54: [SubscribeToStream] Successfully subscribed to streams: [bnbbtc@kline_1m]
2025/07/13 17:01:28 kafkaWriter.go:19: [WriteMessageFromStream] Starting to write messages to Kafka topic: binance.kline at key: bnbbtc
2025/07/13 17:01:29 websocketClient.go:71: [ReadMessages] Received message, passed to channel: {"result":null,"id":1}
2025/07/13 17:01:30 kafkaWriter.go:29: [WriteMessageFromStream] Successfully wrote message to Kafka: {"result":null,"id":1}
2025/07/13 17:01:30 websocketClient.go:71: [ReadMessages] Received message, passed to channel: {"e":"kline","E":1752397290016,"s":"BNBBTC","k":{"t":1752397260000,"T":1752397319999,"s":"BNBBTC","i":"1m","f":273280640,"L":273280663,"o":"0.00585100","c":"0.00585000","h":"0.00585100","l":"0.00584900","v":"5.68800000","n":24,"x":false,"q":"0.03327391","V":"1.25900000","Q":"0.00736515","B":"0"}}
2025/07/13 17:01:31 kafkaWriter.go:29: [WriteMessageFromStream] Successfully wrote message to Kafka: {"e":"kline","E":1752397290016,"s":"BNBBTC","k":{"t":1752397260000,"T":1752397319999,"s":"BNBBTC","i":"1m","f":273280640,"L":273280663,"o":"0.00585100","c":"0.00585000","h":"0.00585100","l":"0.00584900","v":"5.68800000","n":24,"x":false,"q":"0.03327391","V":"1.25900000","Q":"0.00736515","B":"0"}}
```

Start the Flink client which will act as the consumer in the Kafka topic.

```sh
mvn clean compile exec:java
```

Expected logs:

```sh
10> {"result":null,"id":1}
11> {"e":"kline","E":1752397290016,"s":"BNBBTC","k":{"t":1752397260000,"T":1752397319999,"s":"BNBBTC","i":"1m","f":273280640,"L":273280663,"o":"0.00585100","c":"0.00585000","h":"0.00585100","l":"0.00584900","v":"5.68800000","n":24,"x":false,"q":"0.03327391","V":"1.25900000","Q":"0.00736515","B":"0"}}
```

- 10> means subtask 10 of the parallel operator instance printed that log.
  - Flink assigns subtasks (small independent workers) to run parts of the operator logic in parallel.
  - If the operator has a parallelism of 12, you’ll see log lines from subtasks 0>, 1>, ..., 11>.

Query from clickhouse

```sh
docker exec -it clickhouse clickhouse-client --password default
```

```sql
select * from trades.kline_agg
```

Result

```sh
Query id: c6241df8-91aa-4ddb-8e88-10c35c6add6a
   ┌─symbol─┬────────window_start─┬────────────avg_close─┐
1. │ BNBBTC │ 2025-07-13 15:08:00 │ 0.005817222222222223 │
2. │ BNBBTC │ 2025-07-13 15:09:00 │             0.005813 │
3. │ BNBBTC │ 2025-07-13 15:07:00 │ 0.005818750000000001 │
   └────────┴─────────────────────┴──────────────────────┘
   3 rows in set. Elapsed: 0.004 sec.
```
