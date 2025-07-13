package main.java.neozhixuan.flinkproject;

import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class BinanceKafkaConsumerJob {

  public static void main(String[] args) throws Exception {
    // Start a Flink execution environment
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Set up Kafka consumer properties
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("group.id", "flink-binance-consumer");

    // Create a Flink Kafka consumer to read from the "binance.kline" topic
    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
      "binance.kline",
      new SimpleStringSchema(), // Message deserialization schema
      props // Kafka consumer properties
    );

    consumer.setStartFromLatest(); // Start reading from the latest messages

    DataStream<String> stream = env.addSource(consumer); // Add the Kafka consumer as a source to the Flink job

    stream.print(); // Print each message

    // ENTRY: Start the Flink job
    env.execute("Flink Kafka Consumer - Binance");
  }
}
