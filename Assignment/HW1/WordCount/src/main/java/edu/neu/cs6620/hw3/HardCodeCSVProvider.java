package edu.neu.cs6620.hw3;

import static edu.neu.cs6620.hw3.Constants.*;

import java.util.HashMap;
import java.util.Map;

public class HardCodeCSVProvider{

  public final Map<Integer, String> headerMap;

  public HardCodeCSVProvider(){
    this.headerMap = new HashMap<>();
    Map<Integer, String> variableMap = new HashMap<>();
    variableMap.put(0, YEAR);
    variableMap.put(1, QUARTER);
    variableMap.put(2, MONTH);
    variableMap.put(3, DAY_OF_MONTH);
    variableMap.put(4, DAY_OF_WEEK);
    variableMap.put(5, FLIGHT_DATE);
    variableMap.put(6, UNIQUE_CARRIER);
    variableMap.put(7, AIRLINE_ID);
    variableMap.put(8, CARRIER);
    variableMap.put(9, TAIL_NUM);
    variableMap.put(10, FLIGHT_NUM);
    variableMap.put(11, ORIGIN);
    variableMap.put(12, ORIGIN_CITY_NAME);
    variableMap.put(13, ORIGIN_STATE);
    variableMap.put(14, ORIGIN_STATE_FIPS);
    variableMap.put(15, ORIGIN_STATE_NAME);
    variableMap.put(16, ORIGIN_WAC);
    variableMap.put(17, DEST);
    variableMap.put(18, DEST_CITY_NAME);
    variableMap.put(19, DEST_STATE);
    variableMap.put(20, DEST_STATE_FIPS);
    variableMap.put(21, DEST_STATE_NAME);
    variableMap.put(22, DEST_WAC);
    variableMap.put(23, CRS_DEP_TIME);
    variableMap.put(24, DEP_TIME);
    variableMap.put(25, DEP_DELAY);
    variableMap.put(26, DEP_DELAY_MINUTES);
    variableMap.put(27, DEP_DEL_15);
    variableMap.put(28, DEPARTURE_DELAY_GROUPS);
    variableMap.put(29, DEP_TIME_BLK);
    variableMap.put(30, TAXI_OUT);
    variableMap.put(31, WHEELS_OFF);
    variableMap.put(32, WHEELS_ON);
    variableMap.put(33, TAXI_IN);
    variableMap.put(34, CRS_ARR_TIME);
    variableMap.put(35, ARR_TIME);
    variableMap.put(36, ARR_DELAY);
    variableMap.put(37, ARR_DELAY_MINUTES);
    variableMap.put(38, ARR_DEL_15);
    variableMap.put(39, ARRIVAL_DELAY_GROUPS);
    variableMap.put(40, ARR_TIME_BLK);
    variableMap.put(41, CANCELLED);
    variableMap.put(42, CANCELLATION_CODE);
    variableMap.put(43, DIVERTED);
    variableMap.put(44, CRS_ELAPSED_TIME);
    variableMap.put(45, ACTUAL_ELAPSED_TIME);
    variableMap.put(46, AIR_TIME);
    variableMap.put(47, FLIGHTS);
    variableMap.put(48, DISTANCE);
    variableMap.put(49, DISTANCE_GROUP);
    variableMap.put(50, CARRIER_DELAY);
    variableMap.put(51, WEATHER_DELAY);
    variableMap.put(52, NAS_DELAY);
    variableMap.put(53, SECURITY_DELAY);
    variableMap.put(54, LATE_AIRCRAFT_DELAY);

  }

  public Map<Integer, String> provideHeadersMap() {
    return this.headerMap;
  }
}
