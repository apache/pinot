package com.linkedin.thirdeye.anomalydetection.context;

import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.joda.time.Interval;

/**
 * Time series for anomaly detection, which contains the pairs of timestamp and data values from
 * one time series.
 */
public class TimeSeries {

  /**
   * Timestamps to values. Timestamps are sorted.
   */
  private NavigableMap<Long, Double> timeSeries = new TreeMap<>();

  /**
   * The max and min timestamp of this time series; both are inclusive.
   */
  private Interval timeSeriesInterval = new Interval(0L, 0L);


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
   * Returns a sorted timestamp set
   */
  public SortedSet<Long> timestampSet() {
    return timeSeries.navigableKeySet();
  }

  /**
   * Returns the number of timestamps in this time series
   */
  public int size() {
    return timeSeries.size();
  }

  /**
   * Returns the interval of the time series, which provides the max and min timestamps (inclusive).
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

  @Override
  public String toString() {
    return new ToStringBuilder(this).
        append("Interval", timeSeriesInterval).
        append("Time-Values", timeSeries).toString();
  }
}

