package main.java.neozhixuan.flinkproject.utils;

import java.util.Set;
import org.apache.flink.api.java.tuple.Tuple2;

public class SymbolInfo {

  private static final Set<String> KNOWN_QUOTES = Set.of(
    "USDT",
    "BUSD",
    "BTC",
    "ETH",
    "BNB",
    "TRY",
    "TUSD",
    "USDC"
  );

  public static Tuple2<String, String> parseSymbol(String symbol) {
    for (String quote : KNOWN_QUOTES) {
      if (symbol.endsWith(quote)) {
        String base = symbol.substring(0, symbol.length() - quote.length());
        return Tuple2.of(base, quote);
      }
    }
    return Tuple2.of(symbol, "UNKNOWN");
  }
}
