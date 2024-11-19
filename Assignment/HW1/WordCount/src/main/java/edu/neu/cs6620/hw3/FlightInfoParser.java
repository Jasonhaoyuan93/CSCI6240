package edu.neu.cs6620.hw3;

import static edu.neu.cs6620.hw3.Constants.ARR_DELAY_MINUTES;
import static edu.neu.cs6620.hw3.Constants.ARR_TIME;
import static edu.neu.cs6620.hw3.Constants.CANCELLED;
import static edu.neu.cs6620.hw3.Constants.DAY_OF_MONTH;
import static edu.neu.cs6620.hw3.Constants.DEP_TIME;
import static edu.neu.cs6620.hw3.Constants.DEST;
import static edu.neu.cs6620.hw3.Constants.DIVERTED;
import static edu.neu.cs6620.hw3.Constants.FLIGHT_DATE;
import static edu.neu.cs6620.hw3.Constants.MONTH;
import static edu.neu.cs6620.hw3.Constants.ORIGIN;
import static edu.neu.cs6620.hw3.Constants.YEAR;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;

public class FlightInfoParser {

  public Map<String, String> parseLine(String line){
    Map<String, String> parseResult = new HashMap<>();
    String[] parts = line.split(",(?=\\S)");
    parseResult.put(YEAR,clean(parts[0]));
    parseResult.put(MONTH,clean(parts[2]));
    parseResult.put(DAY_OF_MONTH,clean(parts[3]));
    parseResult.put(FLIGHT_DATE,clean(parts[5]));
    parseResult.put(ORIGIN,clean(parts[11]));
    parseResult.put(DEST,clean(parts[17]));
    parseResult.put(DEP_TIME,clean(parts[24]));
    parseResult.put(ARR_TIME,clean(parts[35]));
    parseResult.put(ARR_DELAY_MINUTES,clean(parts[37]));
    parseResult.put(CANCELLED,clean(parts[41]));
    parseResult.put(DIVERTED,clean(parts[43]));
    return parseResult;
  }

  public Text mapComposer(Map<String, String> fieldMap){
    return new Text(fieldMap.get(YEAR) + ","
        + fieldMap.get(MONTH) + ","
        + fieldMap.get(DAY_OF_MONTH) + ","
        + fieldMap.get(FLIGHT_DATE) + ","
        + fieldMap.get(ORIGIN) + ","
        + fieldMap.get(DEST) + ","
        + fieldMap.get(DEP_TIME) + ","
        + fieldMap.get(ARR_TIME) + ","
        + fieldMap.get(ARR_DELAY_MINUTES) + ","
        + fieldMap.get(CANCELLED) + ","
        + fieldMap.get(DIVERTED));
  }

  public Map<String, String> decomposeIntermediateText(Text inputText) {
    Map<String, String> decomposedResult = new HashMap<>();
    String[] parts = inputText.toString().split(",");

    if (parts.length == 11) {
      decomposedResult.put(YEAR, parts[0]);
      decomposedResult.put(MONTH, parts[1]);
      decomposedResult.put(DAY_OF_MONTH, parts[2]);
      decomposedResult.put(FLIGHT_DATE, parts[3]);
      decomposedResult.put(ORIGIN, parts[4]);
      decomposedResult.put(DEST, parts[5]);
      decomposedResult.put(DEP_TIME, parts[6]);
      decomposedResult.put(ARR_TIME, parts[7]);
      decomposedResult.put(ARR_DELAY_MINUTES, parts[8]);
      decomposedResult.put(CANCELLED, parts[9]);
      decomposedResult.put(DIVERTED, parts[10]);
    }

    return decomposedResult;
  }

  private String clean(String field){
    return field.trim().replaceAll("\"", "");
  }

}
