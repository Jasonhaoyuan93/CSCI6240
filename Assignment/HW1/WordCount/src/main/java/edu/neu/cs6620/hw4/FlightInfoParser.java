package edu.neu.cs6620.hw4;

import java.util.HashMap;
import java.util.Map;

public class FlightInfoParser {

  Map<Integer, String> csvProvider;
  public FlightInfoParser(HardCodeCSVProvider csvProvider){
    this.csvProvider = csvProvider.provideHeadersMap();
  }

  public Map<String, String> parseLine(String line){
    Map<String, String> parseResult = new HashMap<>();
    String[] parts = line.split(",(?=\\S)");
    for(int i=0;i<parts.length; i++){
      parseResult.put(csvProvider.get(i),clean(parts[i]));
    }
    return parseResult;
  }

  private String clean(String field){
    return field.trim().replaceAll("\"", "");
  }

}
