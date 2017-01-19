package com.linkedin.thirdeye.anomalydetection.context;

import java.util.Map;
import java.util.Set;
import org.joda.time.Interval;

/**
 * Time series for anomaly detection, which contains the pairs of timestamp and data values from
 * one time series.
 */
public class TimeSeries {

  private Map<Long, Double> timeSeries;
  private Interval timeSeriesInterval;


  /**
   * Returns data value of the specified timestamp
   *
   * @param timestamp the specified timestamp
   *
   * @return data value of the specified timestamp
   */
  public Double get(long timestamp) {
    return timeSeries.get(timestamp);
  }

  /**
   * Sets the data value of the specified timestamp.
   *
   * @param timestamp the specified timestamp.
   * @param value     the data value of the specified timestamp.
   */
  public void set(long timestamp, double value) {
    timeSeries.put(timestamp, value);
  }

  /**
   * Removes data value of the specified timestamp
   */
  public void remove(long timeStamp) {
    timeSeries.remove(timeStamp);
  }

  /**
   * Returns true if the specified timestamp exists
   *
   * @param timestamp the specified timestamp
   *
   * @return true if the specified timestamp exists
   */
  public boolean hasTimestamp(long timestamp) {
    return timeSeries.containsKey(timestamp);
  }

  /**
   * Returns timestamp set
   *
   * @return timestamp set
   */
  public Set<Long> timestampSet() {
    return timeSeries.keySet();
  }

  /**
   * Returns the number of timestamps in this time series
   *
   * @return the number of timestamps in this time series
   */
  public int size() {
    return timeSeries.size();
  }

  /**
   * Returns the interval of the time series, which provides the max and min timestamps.
   */
  public Interval getTimeSeriesInterval() {
    return timeSeriesInterval;
  }

  /**
   * Sets the interval of the time series, which provides the max and min timestamps.
   */
  public void setTimeSeriesInterval(Interval timeSeriesInterval) {
    this.timeSeriesInterval = timeSeriesInterval;
  }
}

