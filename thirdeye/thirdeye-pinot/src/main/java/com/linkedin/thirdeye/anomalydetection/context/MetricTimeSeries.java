package com.linkedin.thirdeye.anomalydetection.context;

import java.util.Set;
import org.joda.time.Interval;


public interface MetricTimeSeries {

  /**
   * Get data value for a given timestamp
   * @param timestamp
   * @return the corresponding value
   */
  Double get(long timestamp);

  /**
   * Remove anomalies data if needed
   */
  void remove(long timeStamp);

  /**
   * Contain timestamp or not
   * @param timestamp
   * @return true or false
   */
  boolean hasTimestamp(long timestamp);

  /**
   * Get timestamp set
   * @return set
   */
  Set<Long> timestampSet();

  /**
   * Returns the interval of the time series, which provides the max and min timestamps (inclusive).
   */
  Interval getTimeSeriesInterval();

  /**
   * Get the size of the timestamp set
   * @return the size of the number of timestamps in the series
   */
  int size();
}
