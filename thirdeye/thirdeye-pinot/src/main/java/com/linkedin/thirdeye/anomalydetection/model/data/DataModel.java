package com.linkedin.thirdeye.anomalydetection.model.data;

import java.util.List;
import java.util.Properties;
import org.joda.time.Interval;

public interface DataModel {
  /**
   * Initializes this model with the given properties.
   * @param properties the given properties.
   */
  void init(Properties properties);

  /**
   * Returns properties of this model.
   */
  Properties getProperties();

  /**
   * Given the interval of the observed (current) time series, returns all intervals of time series
   * that are used by this anomaly detection.
   * @param monitoringWindowStartTime inclusive milliseconds
   * @param monitoringWindowEndTime exclusive milliseconds
   * @return all intervals of time series that are used by this anomaly detection.
   */
  List<Interval> getAllDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);

  /**
   * Given the interval of the observed (current) time series, returns the intervals of time series
   * that are used by this prediction model for training purpose.
   *
   * @param monitoringWindowStartTime inclusive milliseconds
   * @param monitoringWindowEndTime exclusive milliseconds
   *
   * @return intervals of time series that are used for training.
   */
  List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);
}
