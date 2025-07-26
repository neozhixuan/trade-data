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
	binanceScheme      = "wss"
	binanceHost        = "stream.binance.com:9443"
	binancePath        = "/ws/bnbbtc@trade" // Despite using bnbbtc stream, we can also subscribe to other streams
	binanceTradeStream = "bnbbtc@kline_1m"
	tradeChannelLimit  = 1000

	kafkaTopic    = "binance.kline"
	kafkaTopicKey = "bnbbtc"
)

var (
	kafkaBrokers  []string
	kafkaBalancer = &kafka.LeastBytes{}
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
	wssClient, err := websocketClient.NewWebsocketConnection(binanceInfo)
	if err != nil {
		log.Fatalf("Failed to create websocket connection: %v", err)
	}
	defer websocketClient.CloseConnection(wssClient)

	// Subscribe to trade streams within the websocket connection
	streamNames := []string{binanceTradeStream}
	websocketClient.SubscribeToStream(wssClient, streamNames)
	defer websocketClient.UnsubscribeFromStream(wssClient, streamNames)

	// Start a bounded channel to receive trade info
	// - If we exceed tradeChannelLimit, the sender will be blocked unless we use a select-case-default to drop messages
	tradeInfoChannel := make(chan string, tradeChannelLimit)
	defer close(tradeInfoChannel)

	// Read websocket messages in a separate goroutine
	go websocketClient.ReadMessages(wssClient, tradeInfoChannel)

	// Create Kafka writer
	waitForKafka(broker, 10)
	kafkaWriterObj := kafkaWriter.NewKafkaWriter()
	kafkaWriterInstance := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    kafkaTopic,
		Balancer: kafkaBalancer,
	})
	defer kafkaWriterInstance.Close()

	go func() {
		kafkaWriterObj.WriteMessageFromStream(kafkaWriterInstance, tradeInfoChannel, kafkaTopicKey)
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
