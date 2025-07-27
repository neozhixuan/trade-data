package kafkaWriter

import "github.com/segmentio/kafka-go"

type KafkaInfo struct {
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaBalancer kafka.Balancer
}

type KlineMessage struct {
	Symbol string `json:"symbol"`
	// Add more fields as needed
}
