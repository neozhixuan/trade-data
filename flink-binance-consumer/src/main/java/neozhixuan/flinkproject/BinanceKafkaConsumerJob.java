package main.java.neozhixuan.flinkproject;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;
import javax.naming.Context;
import main.java.neozhixuan.flinkproject.pojo.KlineAggRow;
import main.java.neozhixuan.flinkproject.pojo.KlineDataRow;
import main.java.neozhixuan.flinkproject.pojo.KlineEvent;
import main.java.neozhixuan.flinkproject.pojo.KlineEventRow;
import main.java.neozhixuan.flinkproject.pojo.SymbolRow;
import main.java.neozhixuan.flinkproject.pojo.TradeKline;
import main.java.neozhixuan.flinkproject.utils.DeduplicateSymbolFunction;
import main.java.neozhixuan.flinkproject.utils.SymbolInfo;
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
      .map(TradeKline::from) // Use the .from() function to map from KlineEvent to TradeKline
      .returns(TradeKline.class);

    // Parse trade messages into
    DataStream<KlineEvent> tradeStream = consumeKafkaStream
      .filter(jsonStr -> jsonStr.contains("\"e\":\"trade\""))
      .map(jsonStr -> mapper.readValue(jsonStr, KlineEvent.class))
      .returns(KlineEvent.class);

    // Print incoming data
    tradeKlines.print("Parsed KlineEvent to TradeKline");
    tradeStream.print("Parsed TradeEvent");

    // Convert KlineEvent to KlineDataRow
    DataStream<KlineDataRow> klineDataRows = klineStream
      .map(event -> {
        KlineEvent.KlineData k = event.k;
        KlineDataRow row = new KlineDataRow();
        row.symbol = k.s;
        row.interval = k.i;
        row.openTime = new Timestamp(k.t);
        row.closeTime = new Timestamp(k.T);
        row.firstTradeId = k.f;
        row.lastTradeId = k.L;
        row.open = Double.parseDouble(k.o);
        row.close = Double.parseDouble(k.c);
        row.high = Double.parseDouble(k.h);
        row.low = Double.parseDouble(k.l);
        row.volume = Double.parseDouble(k.v);
        row.tradeCount = k.n;
        row.isClosed = k.x;
        row.quoteAssetVolume = Double.parseDouble(k.q);
        row.takerBuyBaseVolume = Double.parseDouble(k.V);
        row.takerBuyQuoteVolume = Double.parseDouble(k.Q);
        row.eventTime = new Timestamp(event.E);
        return row;
      })
      .returns(KlineDataRow.class);

    DataStream<KlineEventRow> klineEventRows = klineStream
      .map(event -> {
        KlineEventRow row = new KlineEventRow();
        row.eventType = event.e;
        row.eventTime = new Timestamp(event.E);
        row.symbol = event.s;
        return row;
      })
      .returns(KlineEventRow.class);

    DataStream<SymbolRow> uniqueSymbols = klineStream
      .map(event -> {
        Tuple2<String, String> baseQuote = SymbolInfo.parseSymbol(event.s);
        SymbolRow row = new SymbolRow();
        row.symbol = event.s;
        row.baseAsset = baseQuote.f0;
        row.quoteAsset = baseQuote.f1;
        row.assetClass = "crypto"; // hardcoded for now
        row.launched = new Date(System.currentTimeMillis()); // can be refined
        return row;
      })
      .returns(SymbolRow.class)
      .keyBy(row -> row.symbol)
      .process(new DeduplicateSymbolFunction());

    // Windowed average of the close price from incoming TradeKline events
    // - grouped by symbol (BNBBTC)
    // - event time tumbling windows of 1 minute
    DataStream<KlineAggRow> aggregated = tradeKlines
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
        // We manipulate the output from the above function into a KlineAggRow object
        new ProcessWindowFunction<Double, KlineAggRow, String, TimeWindow>() {
          @Override
          public void process(
            String key, // Group key (symbol, e.g. BNBBTC)
            Context context, // We can access window start and end
            Iterable<Double> elements, // The aggregated average from the previous step
            Collector<KlineAggRow> out // The result
          ) {
            Double avgClose = elements.iterator().next();
            long windowStart = context.window().getStart();
            out.collect(new KlineAggRow(key, windowStart / 1000, avgClose));
          }
        } // OUTPUT: DataStream<KlineAggRow> with fields: symbol, windowStart (in seconds), avgClose
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

    klineDataRows.addSink(
      JdbcSink.sink(
        "INSERT INTO kline_data (symbol, interval, open_time, close_time, first_trade_id, last_trade_id, open, close, high, low, volume, trade_count, is_closed, quote_asset_volume, taker_buy_base_volume, taker_buy_quote_volume, event_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (ps, t) -> {
          ps.setString(1, t.symbol);
          ps.setString(2, t.interval);
          ps.setTimestamp(3, t.openTime);
          ps.setTimestamp(4, t.closeTime);
          ps.setLong(5, t.firstTradeId);
          ps.setLong(6, t.lastTradeId);
          ps.setDouble(7, t.open);
          ps.setDouble(8, t.close);
          ps.setDouble(9, t.high);
          ps.setDouble(10, t.low);
          ps.setDouble(11, t.volume);
          ps.setInt(12, t.tradeCount);
          ps.setBoolean(13, t.isClosed);
          ps.setDouble(14, t.quoteAssetVolume);
          ps.setDouble(15, t.takerBuyBaseVolume);
          ps.setDouble(16, t.takerBuyQuoteVolume);
          ps.setTimestamp(17, t.eventTime);
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

    klineEventRows.addSink(
      JdbcSink.sink(
        "INSERT INTO kline_events (event_type, event_time, symbol) VALUES (?, ?, ?)",
        (ps, t) -> {
          ps.setString(1, t.eventType);
          ps.setTimestamp(2, t.eventTime);
          ps.setString(3, t.symbol);
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

    uniqueSymbols.addSink(
      JdbcSink.sink(
        "INSERT INTO symbols (symbol, base_asset, quote_asset, asset_class, launched) VALUES (?, ?, ?, ?, ?)",
        (ps, t) -> {
          ps.setString(1, t.symbol);
          ps.setString(2, t.baseAsset);
          ps.setString(3, t.quoteAsset);
          ps.setString(4, t.assetClass);
          ps.setDate(5, t.launched);
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

    streamEnv.setParallelism(2).execute("Flink Kafka Consumer - Binance");
  }
}
