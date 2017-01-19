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
   * Given the interval of the observed (current) time series, returns the intervals of time series
   * that are used by this prediction model for training purpose.
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return intervals of time series that are used for training.
   */
  List<Interval> getTrainingDataIntervals(long monitoringWindowStartTime, long monitoringWindowEndTime);
}
