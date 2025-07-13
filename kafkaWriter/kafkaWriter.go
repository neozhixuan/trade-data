package kafkawriter

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type kafkaWriter struct {
}

func NewKafkaWriter() *kafkaWriter {
	return &kafkaWriter{}
}

func (k *kafkaWriter) WriteMessageFromStream(writer *kafka.Writer, tradeChannel chan string, topicKey string) error {
	// No infinite loop needed, to read from a single channel. This is more idiomatic in Go, because it only exits when channel CLOSES.
	log.Printf("[WriteMessageFromStream] Starting to write messages to Kafka topic: %s at key: %s", writer.Topic, topicKey)
	for tradeInfo := range tradeChannel {
		msg := kafka.Message{
			Key:   []byte(topicKey),
			Value: []byte(tradeInfo),
		}
		if err := writer.WriteMessages(context.Background(), msg); err != nil {
			log.Printf("[WriteMessageFromStream] Failed to write message to Kafka: %s", err.Error())
			return err
		}
		log.Printf("[WriteMessageFromStream] Successfully wrote message to Kafka: %s", tradeInfo)
	}
	return nil
}
