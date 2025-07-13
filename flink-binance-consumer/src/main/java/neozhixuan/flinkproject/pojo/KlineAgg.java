package main.java.neozhixuan.flinkproject.pojo;

public class KlineAgg {

  public String symbol;
  public long windowStart;
  public double avgClose;

  public KlineAgg() {}

  public KlineAgg(String symbol, long windowStart, double avgClose) {
    this.symbol = symbol;
    this.windowStart = windowStart;
    this.avgClose = avgClose;
  }
}
