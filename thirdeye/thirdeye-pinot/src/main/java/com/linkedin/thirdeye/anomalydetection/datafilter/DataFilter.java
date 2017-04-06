package com.linkedin.thirdeye.anomalydetection.datafilter;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import java.util.Map;

public interface DataFilter {

  /**
   * Sets parameters for this filter.
   *
   * @param props the properties for this filter.
   */
  void setParameters(Map<String, String> props);

  /**
   * Returns if the given data, which is a time series with the given metric name and dimension map, passes the filter.
   *
   * @param metricTimeSeries the context for retrieving the time series.
   * @param dimensionMap     the dimension map to retrieve the time series.
   *
   * @return true if the time series passes this filter.
   */
  boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap);

  /**
   * Returns if the given time series, which is specified by a metric name, a dimension map, and a pair of
   * start and end timestamps, passes the filter.
   *
   * @param metricTimeSeries the context for retrieving the time series.
   * @param dimensionMap     the dimension map of time series.
   * @param windowStart      the start timestamp of the target data points, inclusive.
   * @param windowEnd        the end timestamp of the target data points, exclusive.
   *
   * @return true if the time series passes this filter.
   */
  boolean isQualified(MetricTimeSeries metricTimeSeries, DimensionMap dimensionMap, long windowStart, long windowEnd);
}
