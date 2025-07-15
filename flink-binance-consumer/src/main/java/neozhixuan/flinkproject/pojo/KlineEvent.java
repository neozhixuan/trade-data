package main.java.neozhixuan.flinkproject.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import main.java.neozhixuan.flinkproject.pojo.KlineEvent.KlineData;

// Allow fields that are empty/not present in the JSON to be ignored
@JsonIgnoreProperties(ignoreUnknown = true)
public class KlineEvent extends BaseEvent {

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
