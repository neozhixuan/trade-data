package main.java.neozhixuan.flinkproject.pojo;

public class TradeEvent extends BaseEvent {

  public long t; // trade ID
  public String p; // price
  public String q; // quantity
  public long T; // trade time
  public boolean m; // is buyer market maker
  public boolean M;
}
