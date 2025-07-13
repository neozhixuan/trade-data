# Data Engineering Pipeline

This project outlines a test pipeline to read live trade data from Binance WebSocket into a Kafka MQ, and process this stream of data using Apache Flink.

## Setup

Start docker services for Zookeeper and Kafka

```sh
docker-compose up -d # Run containers in detached mode
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
