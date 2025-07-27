package main.java.neozhixuan.flinkproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Properties;
import javax.naming.Context;
import main.java.neozhixuan.flinkproject.pojo.KlineAgg;
import main.java.neozhixuan.flinkproject.pojo.KlineEvent;
import main.java.neozhixuan.flinkproject.pojo.TradeKline;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

public class BinanceKafkaConsumerJob {

  public static void main(String[] args) throws Exception {
    // Start a Flink execution environment
    final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    // Set up Kafka consumer properties
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "kafka:9092");
    props.setProperty("group.id", "flink-binance-consumer");

    // Create a Flink Kafka consumer to read from the "binance.kline" topic
    FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
      "binance.kline",
      new SimpleStringSchema(), // Message deserialization schema
      props // Kafka consumer properties
    );

    // Start reading from the latest messages
    consumer.setStartFromLatest();

    DataStream<String> consumeKafkaStream = streamEnv.addSource(consumer);

    // Use Jackson library to parse JSON messages
    ObjectMapper mapper = new ObjectMapper();
    // Parse stream messages into KlineEvent objects
    DataStream<KlineEvent> klineStream = consumeKafkaStream
      .filter(jsonStr -> jsonStr.contains("\"e\":\"kline\""))
      .map(jsonStr -> mapper.readValue(jsonStr, KlineEvent.class))
      .returns(KlineEvent.class);

    // Parse KlineEvent into TradeKline
    DataStream<TradeKline> tradeKlines = klineStream
      .map(TradeKline::from)
      .returns(TradeKline.class);

    // Parse trade messages into
    DataStream<KlineEvent> tradeStream = consumeKafkaStream
      .filter(jsonStr -> jsonStr.contains("\"e\":\"trade\""))
      .map(jsonStr -> mapper.readValue(jsonStr, KlineEvent.class))
      .returns(KlineEvent.class);

    // Print incoming data
    tradeKlines.print("Parsed KlineEvent to TradeKline");
    tradeStream.print("Parsed TradeEvent");

    // Windowed average of the close price from incoming TradeKline events
    // - grouped by symbol (BNBBTC)
    // - event time tumbling windows of 1 minute
    DataStream<KlineAgg> aggregated = tradeKlines
      // Tells Flink how to extract event time and generate watermarks
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .<TradeKline>forMonotonousTimestamps() // Assumes that timestamps are always increasing (works for trade data)
          .withTimestampAssigner((tk, ts) -> tk.closeTime) // Extracts the event timestamp (tk.closeTime) from each TradeKline
          .withIdleness(Duration.ofSeconds(5)) // Mark a source as idle after 5 seconds of no data, so watermark can progress in parallel sources
      )
      .keyBy(tk -> tk.symbol) // Group the streams (each group has their own window) by "tk.symbol" eg. BNBBTC
      // Create non-overlapping windows of 1 minute of event time
      // - A new window starts every minute based on event time a.k.a closeTime (10:01:00–10:02:00, 10:02:00–10:03:00)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      // Compute average close price for a window
      .aggregate(
        new AggregateFunction<TradeKline, Tuple2<Double, Integer>, Double>() {
          // Initialise the accumulator
          // Tuple2<sum of close prices, count of trades>
          public Tuple2<Double, Integer> createAccumulator() {
            return Tuple2.of(0.0, 0);
          }

          // For each tradeKline in window
          // - add its close price to the accumulator
          // - increment the count of trades
          public Tuple2<Double, Integer> add(
            TradeKline value,
            Tuple2<Double, Integer> acc
          ) {
            return Tuple2.of(acc.f0 + value.close, acc.f1 + 1);
          }

          // At the end, get average
          public Double getResult(Tuple2<Double, Integer> acc) {
            return acc.f1 == 0 ? 0.0 : acc.f0 / acc.f1;
          }

          // Merges two accumulators (session/custom windows, or parallelism)
          public Tuple2<Double, Integer> merge(
            Tuple2<Double, Integer> a,
            Tuple2<Double, Integer> b
          ) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
          }
        },
        // We manipulate the output from the above function into a KlineAgg object
        new ProcessWindowFunction<Double, KlineAgg, String, TimeWindow>() {
          @Override
          public void process(
            String key, // Group key (symbol, e.g. BNBBTC)
            Context context, // We can access window start and end
            Iterable<Double> elements, // The aggregated average from the previous step
            Collector<KlineAgg> out // The result
          ) {
            Double avgClose = elements.iterator().next();
            long windowStart = context.window().getStart();
            out.collect(new KlineAgg(key, windowStart / 1000, avgClose));
          }
        } // OUTPUT: DataStream<KlineAgg> with fields: symbol, windowStart (in seconds), avgClose
      );

    // Use the modern JdbcSink.sink
    aggregated.addSink(
      JdbcSink.sink(
        "INSERT INTO kline_agg (symbol, window_start, avg_close) VALUES (?, ?, ?)",
        (ps, t) -> {
          ps.setString(1, t.symbol);
          ps.setTimestamp(2, new java.sql.Timestamp(t.windowStart * 1000)); // windowStart is in seconds
          ps.setDouble(3, t.avgClose);
        },
        JdbcExecutionOptions
          .builder()
          .withBatchSize(5)
          .withBatchIntervalMs(200)
          .withMaxRetries(3)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:clickhouse://clickhouse:8123/trades")
          .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
          .withUsername("default")
          .withPassword("default")
          .build()
      )
    );

    streamEnv.setParallelism(1).execute("Flink Kafka Consumer - Binance");
  }
}
