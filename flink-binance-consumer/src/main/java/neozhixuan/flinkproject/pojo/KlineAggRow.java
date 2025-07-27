package main.java.neozhixuan.flinkproject.pojo;

public class KlineAggRow {

  public String symbol;
  public long windowStart;
  public double avgClose;

  public KlineAggRow() {}

  public KlineAggRow(String symbol, long windowStart, double avgClose) {
    this.symbol = symbol;
    this.windowStart = windowStart;
    this.avgClose = avgClose;
  }
}
