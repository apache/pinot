package com.linkedin.thirdeye.anomaly.events;

import java.util.List;
import java.util.Map;

public abstract class ExternalEvent {
  long startTime;
  long endTime;
  Map<String, List<String>> dimensionValuesMap;

  public Map<String, List<String>> getDimensionValuesMap() {
    return dimensionValuesMap;
  }

  public void setDimensionValuesMap(Map<String, List<String>> dimensionValuesMap) {
    this.dimensionValuesMap = dimensionValuesMap;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
