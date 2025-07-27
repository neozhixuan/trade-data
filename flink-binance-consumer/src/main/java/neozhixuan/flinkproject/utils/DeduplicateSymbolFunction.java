package main.java.neozhixuan.flinkproject.utils;

import main.java.neozhixuan.flinkproject.pojo.SymbolRow;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeduplicateSymbolFunction
  extends KeyedProcessFunction<String, SymbolRow, SymbolRow> {

  private transient ValueState<Boolean> seen;

  @Override
  public void open(Configuration parameters) {
    seen =
      getRuntimeContext()
        .getState(new ValueStateDescriptor<>("seen", Boolean.class));
  }

  @Override
  public void processElement(
    SymbolRow value,
    Context ctx,
    Collector<SymbolRow> out
  ) throws Exception {
    if (seen.value() == null) {
      seen.update(true);
      out.collect(value); // Only emit first time
    }
  }
}
