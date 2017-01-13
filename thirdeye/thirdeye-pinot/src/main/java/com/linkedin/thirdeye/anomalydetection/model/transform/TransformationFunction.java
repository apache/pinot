package com.linkedin.thirdeye.anomalydetection.model.transform;

import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;

/**
 * A stateless transformation function for generating the transformed time series from the given
 * time series.
 */
public interface TransformationFunction {
  /**
   * Returns a time series that is transformed from the given time series. The input time series
   * is not modified.
   *
   * @param timeSeries the time series that provides the data points to be transformed.
   * @return a time series that is transformed from the given time series.
   */
  TimeSeries transform(TimeSeries timeSeries);
}
