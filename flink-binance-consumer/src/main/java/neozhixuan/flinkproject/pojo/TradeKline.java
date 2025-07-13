package main.java.neozhixuan.flinkproject.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Allow fields that are empty/not present in the JSON to be ignored
@JsonIgnoreProperties(ignoreUnknown = true)
public class TradeKline {

  public String symbol;
  public long openTime;
  public long closeTime;
  public double open;
  public double close;
  public double high;
  public double low;
  public double volume;

  public static TradeKline from(KlineEvent klineEvent) {
    KlineEvent.KlineData k = klineEvent.k;
    TradeKline tk = new TradeKline();
    tk.symbol = k.s;
    tk.openTime = k.t;
    tk.closeTime = k.T;
    tk.open = Double.parseDouble(k.o);
    tk.close = Double.parseDouble(k.c);
    tk.high = Double.parseDouble(k.h);
    tk.low = Double.parseDouble(k.l);
    tk.volume = Double.parseDouble(k.v);
    return tk;
  }

  // Override toString to log the object into a readable string format
  @Override
  public String toString() {
    return (
      "TradeKline{" +
      "symbol='" +
      symbol +
      '\'' +
      ", close=" +
      close +
      ", closeTime=" +
      closeTime +
      ", volume=" +
      volume +
      '}'
    );
  }
}
