package kafkaWriter

import "github.com/segmentio/kafka-go"

type KafkaInfo struct {
	KafkaBrokers  []string
	KafkaTopic    string
	KafkaBalancer kafka.Balancer
}
