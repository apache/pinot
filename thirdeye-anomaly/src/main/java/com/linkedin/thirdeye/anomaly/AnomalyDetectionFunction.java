package com.linkedin.thirdeye.anomaly;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

import java.util.Properties;

public interface AnomalyDetectionFunction
{
  /**
   * Initializes this function with the star tree config and arbitrary function config
   */
  void init(StarTreeConfig starTreeConfig, Properties functionConfig);

  /**
   * @return
   *  The minimum length of time needed to perform the anomaly detection function (null == all time)
   */
  TimeGranularity getWindowTimeGranularity();

  /**
   * Computes whether or not this dimension combination + metric time series is anomalous.
   */
  AnomalyResult analyze(DimensionKey dimensionKey, MetricTimeSeries metricTimeSeries);
}
