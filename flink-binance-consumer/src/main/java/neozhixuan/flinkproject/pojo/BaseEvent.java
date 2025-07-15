package main.java.neozhixuan.flinkproject.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Allow fields that are empty/not present in the JSON to be ignored
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseEvent {

  public String e; // event type
  public long E; // event time
  public String s; // symbol
}
