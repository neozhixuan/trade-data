package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	kafkawriter "trade-data/kafkaWriter"
	"trade-data/websocketClient"

	"github.com/segmentio/kafka-go"
)

const (
	binanceScheme     = "wss"
	binanceHost       = "stream.binance.com:9443"
	binancePath       = "/ws/bnbbtc@trade"
	tradeChannelLimit = 100

	kafkaTopic    = "binance.kline"
	kafkaTopicKey = "bnbbtc"
)

var (
	kafkaBrokers  = []string{"localhost:9092"}
	kafkaBalancer = &kafka.LeastBytes{}
)

func main() {
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

	// Subscribe to trade streams
	streamNames := []string{"bnbbtc@kline_1m"}
	websocketClient.SubscribeToStream(wssClient, streamNames)

	// Start a bounded channel to receive trade info
	// - If we exceed tradeChannelLimit, the sender will be blocked unless we use a select-case-default to drop messages
	tradeInfoChannel := make(chan string, tradeChannelLimit)
	defer close(tradeInfoChannel)

	// Read websocket messages in a separate goroutine
	go websocketClient.ReadMessages(wssClient, tradeInfoChannel)

	// Create Kafka writer
	kafkaWriter := kafkawriter.NewKafkaWriter()
	kafkaWriterInstance := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  kafkaBrokers,
		Topic:    kafkaTopic,
		Balancer: kafkaBalancer,
	})
	defer kafkaWriterInstance.Close()

	go kafkaWriter.WriteMessageFromStream(kafkaWriterInstance, tradeInfoChannel, kafkaTopicKey)

	// Wait for interrupt signal (program exited) to gracefully shutdown and call all close functions
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // os.Interrupt triggers on Ctrl+C; syscall.SIGTERM on termination
	<-sigChan
	log.Println("[main] Interrupt signal detected, shutting down...")
}
