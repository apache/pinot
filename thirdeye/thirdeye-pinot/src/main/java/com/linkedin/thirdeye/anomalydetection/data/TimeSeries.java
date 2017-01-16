package com.linkedin.thirdeye.anomalydetection.data;

import java.util.Map;
import java.util.Set;

/**
 * Time series for anomaly detection, which contains the pairs of timestamp and data values from
 * one time series.
 */
public class TimeSeries {

  private Map<Long, Double> timeSeries;


  /**
   * Returns data value of the specified timestamp
   *
   * @param timestamp the specified timestamp
   *
   * @return data value of the specified timestamp
   */
  Double get(long timestamp) {
    return timeSeries.get(timestamp);
  }

  /**
   * Sets the data value of the specified timestamp.
   *
   * @param timestamp the specified timestamp.
   * @param value     the data value of the specified timestamp.
   */
  void set(long timestamp, double value) {
    timeSeries.put(timestamp, value);
  }

  /**
   * Removes data value of the specified timestamp
   */
  void remove(long timeStamp) {
    timeSeries.remove(timeStamp);
  }

  /**
   * Returns true if the specified timestamp exists
   *
   * @param timestamp the specified timestamp
   *
   * @return true if the specified timestamp exists
   */
  boolean hasTimestamp(long timestamp) {
    return timeSeries.containsKey(timestamp);
  }

  /**
   * Returns timestamp set
   *
   * @return timestamp set
   */
  Set<Long> timestampSet() {
    return timeSeries.keySet();
  }

  /**
   * Returns the number of timestamps in this time series
   *
   * @return the number of timestamps in this time series
   */
  int size() {
    return timeSeries.size();
  }
}

