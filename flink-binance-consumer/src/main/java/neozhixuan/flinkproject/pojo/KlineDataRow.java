package main.java.neozhixuan.flinkproject.pojo;

import java.sql.Timestamp;

public class KlineDataRow {

  public String symbol;
  public String interval;
  public Timestamp openTime;
  public Timestamp closeTime;
  public long firstTradeId;
  public long lastTradeId;
  public double open;
  public double close;
  public double high;
  public double low;
  public double volume;
  public int tradeCount;
  public boolean isClosed;
  public double quoteAssetVolume;
  public double takerBuyBaseVolume;
  public double takerBuyQuoteVolume;
  public Timestamp eventTime;

  public KlineDataRow() {}
}
