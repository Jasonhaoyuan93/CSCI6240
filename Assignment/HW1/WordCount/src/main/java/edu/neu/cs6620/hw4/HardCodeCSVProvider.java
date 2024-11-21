package edu.neu.cs6620.hw4;

import java.util.HashMap;
import java.util.Map;

import static edu.neu.cs6620.hw3.Constants.*;

public class HardCodeCSVProvider {

  private final Map<Integer, String> headerMap;

  public HardCodeCSVProvider(){
    this.headerMap = new HashMap<>();
    headerMap.put(0, YEAR);
    headerMap.put(1, QUARTER);
    headerMap.put(2, MONTH);
    headerMap.put(3, DAY_OF_MONTH);
    headerMap.put(4, DAY_OF_WEEK);
    headerMap.put(5, FLIGHT_DATE);
    headerMap.put(6, UNIQUE_CARRIER);
    headerMap.put(7, AIRLINE_ID);
    headerMap.put(8, CARRIER);
    headerMap.put(9, TAIL_NUM);
    headerMap.put(10, FLIGHT_NUM);
    headerMap.put(11, ORIGIN);
    headerMap.put(12, ORIGIN_CITY_NAME);
    headerMap.put(13, ORIGIN_STATE);
    headerMap.put(14, ORIGIN_STATE_FIPS);
    headerMap.put(15, ORIGIN_STATE_NAME);
    headerMap.put(16, ORIGIN_WAC);
    headerMap.put(17, DEST);
    headerMap.put(18, DEST_CITY_NAME);
    headerMap.put(19, DEST_STATE);
    headerMap.put(20, DEST_STATE_FIPS);
    headerMap.put(21, DEST_STATE_NAME);
    headerMap.put(22, DEST_WAC);
    headerMap.put(23, CRS_DEP_TIME);
    headerMap.put(24, DEP_TIME);
    headerMap.put(25, DEP_DELAY);
    headerMap.put(26, DEP_DELAY_MINUTES);
    headerMap.put(27, DEP_DEL_15);
    headerMap.put(28, DEPARTURE_DELAY_GROUPS);
    headerMap.put(29, DEP_TIME_BLK);
    headerMap.put(30, TAXI_OUT);
    headerMap.put(31, WHEELS_OFF);
    headerMap.put(32, WHEELS_ON);
    headerMap.put(33, TAXI_IN);
    headerMap.put(34, CRS_ARR_TIME);
    headerMap.put(35, ARR_TIME);
    headerMap.put(36, ARR_DELAY);
    headerMap.put(37, ARR_DELAY_MINUTES);
    headerMap.put(38, ARR_DEL_15);
    headerMap.put(39, ARRIVAL_DELAY_GROUPS);
    headerMap.put(40, ARR_TIME_BLK);
    headerMap.put(41, CANCELLED);
    headerMap.put(42, CANCELLATION_CODE);
    headerMap.put(43, DIVERTED);
    headerMap.put(44, CRS_ELAPSED_TIME);
    headerMap.put(45, ACTUAL_ELAPSED_TIME);
    headerMap.put(46, AIR_TIME);
    headerMap.put(47, FLIGHTS);
    headerMap.put(48, DISTANCE);
    headerMap.put(49, DISTANCE_GROUP);
    headerMap.put(50, CARRIER_DELAY);
    headerMap.put(51, WEATHER_DELAY);
    headerMap.put(52, NAS_DELAY);
    headerMap.put(53, SECURITY_DELAY);
    headerMap.put(54, LATE_AIRCRAFT_DELAY);

  }

  public Map<Integer, String> provideHeadersMap() {
    return this.headerMap;
  }
}
