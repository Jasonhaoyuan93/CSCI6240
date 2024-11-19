package edu.neu.cs6620.hw3;

import static edu.neu.cs6620.hw3.Constants.*;

import java.util.Map;

public class FlightFilter {

  private final int startYear;
  private final int startMonth;
  private final int endYear;
  private final int endMonth;

  public FlightFilter(int startYear, int startMonth, int endYear, int endMonth) {
    this.startYear = startYear;
    this.startMonth = startMonth;
    this.endYear = endYear;
    this.endMonth = endMonth;
  }


  public boolean isValid(Map<String, String> fieldMap){
    try{
      if(!timeRangeCheck(fieldMap)) {
        return false;
      }

      if(!ORD.equalsIgnoreCase(fieldMap.get(ORIGIN))&&!JFK.equalsIgnoreCase(fieldMap.get(DEST))){
        return false;
      }

      if(ORD.equalsIgnoreCase(fieldMap.get(ORIGIN)) && JFK.equalsIgnoreCase(fieldMap.get(DEST))){
        return false;
      }

      if(Double.parseDouble(fieldMap.get(CANCELLED))>0||Double.parseDouble(fieldMap.get(DIVERTED))>0){
        return false;
      }

      return true;
    }catch (Exception e){
      return false;
    }
  }

  private boolean timeRangeCheck(Map<String, String> fieldMap) {
    int year = Integer.parseInt(fieldMap.get(YEAR));
    if(year<startYear||year>endYear){
      return false;
    }
    int month = Integer.parseInt(fieldMap.get(MONTH));
    if(year==startYear&&month<startMonth){
      return false;
    }
    if(year==endYear&&month>endMonth){
      return false;
    }
    return true;
  }

}
