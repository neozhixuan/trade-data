package main

import (
	"data-ingest/kafkaWriter"
	"data-ingest/websocketClient"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	binanceScheme     = "wss"
	binanceHost       = "stream.binance.com:9443"
	binancePath       = "/ws/bnbbtc@trade" // Despite using bnbbtc stream, we can also subscribe to other streams
	tradeChannelLimit = 1000

	kafkaTopic = "binance.kline"
)

var (
	kafkaBrokers                   []string
	kafkaBalancer                  = &kafka.LeastBytes{}
	binanceKlineStreamsToSubscribe = []string{"ethusdt@kline_1m", "btcusdt@kline_1m"}
)

func main() {
	// Env var from docker/env
	broker := os.Getenv("KAFKA_BROKER")
	kafkaBrokers = []string{broker}

	// Set up logger with file name and line number
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// Start websocket client to Binance
	binanceInfo := websocketClient.BinanceInfo{
		BinanceScheme: binanceScheme,
		BinanceHost:   binanceHost,
		BinancePath:   binancePath,
	}
	websocketClient := websocketClient.NewWebsocketClient()
	if err := websocketClient.NewWebsocketConnection(binanceInfo); err != nil {
		log.Fatalf("Failed to create websocket connection: %v", err)
	}
	defer websocketClient.CloseConnection()

	// Subscribe to trade streams within the websocket connection
	websocketClient.SubscribeToStreams(binanceKlineStreamsToSubscribe)
	defer websocketClient.UnsubscribeFromStreams(binanceKlineStreamsToSubscribe)

	// Start a bounded channel to receive trade info
	// - If we exceed tradeChannelLimit, the sender will be blocked unless we use a select-case-default to drop messages
	tradeInfoChannel := make(chan string, tradeChannelLimit)
	defer close(tradeInfoChannel)

	// Read websocket messages in a separate goroutine
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[main] Recovered from panic when reading messages from channel: %v", r)
			}
		}()

		websocketClient.ReadMessages(tradeInfoChannel)
	}()

	// Create Kafka writer
	waitForKafka(broker, 10)
	kafkaWriterObj := kafkaWriter.NewKafkaWriter()
	kafkaWriterInstance := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    kafkaTopic, // Write all Kline messages to the Kline topic
		Balancer: kafkaBalancer,
	})
	defer kafkaWriterInstance.Close()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[main] Recovered from panic when writing messages to Kafka: %v", r)
			}
		}()

		if err := kafkaWriterObj.WriteMessageFromStream(kafkaWriterInstance, tradeInfoChannel); err != nil {
			log.Printf("[main] Error writing messages to Kafka: %v", err)
		}
	}()

	// Wait for interrupt signal (program exited) to gracefully shutdown and call all close functions
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // os.Interrupt triggers on Ctrl+C; syscall.SIGTERM on termination
	<-sigChan
	log.Println("[main] Interrupt signal detected, shutting down...")
}

func waitForKafka(broker string, maxRetries int) {
	for i := 0; i < maxRetries; i++ {
		// Dial the Kafka broker via TCP every 2 seconds for X retries max
		conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
		if err == nil {
			conn.Close()
			log.Println("[Kafka Ready]")
			return
		}
		log.Printf("[Kafka Wait] Retrying... (%d/%d)\n", i+1, maxRetries)
		time.Sleep(2 * time.Second)
	}
	log.Fatalf("Kafka not available after %d retries", maxRetries)
}
