package kafkaWriter

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

type kafkaWriter struct {
}

func NewKafkaWriter() *kafkaWriter {
	return &kafkaWriter{}
}

// For each kline message, we write to Kafka with the symbol (e.g. ETHUSDT) as the key
func (k *kafkaWriter) WriteMessageFromStream(writer *kafka.Writer, tradeChannel chan string) error {
	// No infinite loop needed, to read from a single channel. This is more idiomatic in Go, because it only exits when channel CLOSES.
	for tradeInfo := range tradeChannel {
		if tradeInfo == "" {
			log.Println("[WriteMessageFromStream] Received empty trade info, skipping...")
			continue
		}

		var klineMessage KlineMessage
		// Extract the symbol from the trade info to use as partition key
		if err := json.Unmarshal([]byte(tradeInfo), &klineMessage); err != nil {
			log.Printf("[WriteMessageFromStream] Failed to unmarshal trade info: %s", err.Error())
			continue
		}

		msg := kafka.Message{
			Key:   []byte(klineMessage.Symbol),
			Value: []byte(tradeInfo),
		}
		log.Printf("[WriteMessageFromStream] Writing to Kafka at topic key %s: %s", klineMessage.Symbol, tradeInfo)
		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Printf("[WriteMessageFromStream] Failed to write message to Kafka: %s", err.Error())
			return err
		}
		log.Printf("[WriteMessageFromStream] Successfully wrote message to Kafka: %s", tradeInfo)
	}
	return nil
}
