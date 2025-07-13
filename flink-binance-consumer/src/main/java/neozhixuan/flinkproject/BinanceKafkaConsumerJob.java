package main.java.neozhixuan.flinkproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Properties;
import main.java.neozhixuan.flinkproject.pojo.KlineEvent;
import main.java.neozhixuan.flinkproject.pojo.TradeKline;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

    // Start reading from the latest messages
    consumer.setStartFromLatest();

    // Use Jackson library to parse JSON messages
    ObjectMapper mapper = new ObjectMapper();

    DataStream<String> tradeStream = env.addSource(consumer);

    // Parse stream messages into KlineEvent objects
    DataStream<KlineEvent> klineStream = tradeStream
      .filter(jsonStr -> jsonStr.contains("\"e\":\"kline\""))
      .map(jsonStr -> mapper.readValue(jsonStr, KlineEvent.class))
      .returns(KlineEvent.class);

    // Parase KlineEvent into TradeKline
    DataStream<TradeKline> tradeKlines = klineStream
      .map(TradeKline::from)
      .returns(TradeKline.class);

    // Print incoming data
    tradeKlines.print("Parsed TradeKline");

    // Windowed average using AggregateFunction
    tradeKlines
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .<TradeKline>forMonotonousTimestamps()
          .withTimestampAssigner((tk, ts) -> tk.closeTime)
          .withIdleness(Duration.ofSeconds(5)) // Mark idle sources so watermark can progress
      )
      .keyBy(tk -> tk.symbol) // Group the streams by "tk.symbol" eg. BNBBTC
      .window(TumblingEventTimeWindows.of(Time.minutes(1))) // Create non-overlapping windows of 1 minute of event time
      .aggregate(
        new AggregateFunction<TradeKline, Tuple2<Double, Integer>, String>() {
          public Tuple2<Double, Integer> createAccumulator() { // SUm of close prices and count
            return Tuple2.of(0.0, 0);
          }

          public Tuple2<Double, Integer> add( // Sum up all close prices and count
            TradeKline value,
            Tuple2<Double, Integer> acc
          ) {
            return Tuple2.of(acc.f0 + value.close, acc.f1 + 1);
          }

          public String getResult(Tuple2<Double, Integer> acc) {
            double avg = acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1;
            String result =
              "Window Avg Close = " + avg + " based on count=" + acc.f1;
            System.out.println("[DEBUG] Emitting window result: " + result);
            return result;
          }

          public Tuple2<Double, Integer> merge( // Merge the accumulators (for parallelism)
            Tuple2<Double, Integer> a,
            Tuple2<Double, Integer> b
          ) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
          }
        }
      )
      .print("Aggregate");

    env.setParallelism(1).execute("Flink Kafka Consumer - Binance");
  }
}
