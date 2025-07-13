package main.java.neozhixuan.flinkproject.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KlineEvent {

  public String e; // event type
  public long E; // event time
  public String s; // symbol
  public KlineData k;

  public static class KlineData {

    public long t; // start time
    public long T; // end time
    public String s; // symbol
    public String i; // interval
    public long f; // first trade ID
    public long L; // last trade ID
    public String o; // open price
    public String c; // close price
    public String h; // high price
    public String l; // low price
    public String v; // volume
    public int n; // trade count
    public boolean x; // is this kline closed?
    public String q; // quote asset volume
    public String V; // taker buy base volume
    public String Q; // taker buy quote volume
    public String B; // ignore
  }
}
