package com.linkedin.thirdeye.anomaly.api.external;

import java.util.List;
import java.util.Set;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

public interface AnomalyDetectionFunction {

  /**
   * Handle any initialization of the function. This is guaranteed to be called before any of the subsequent methods.
   *
   * @param starTreeConfig
   * @param functionConfig
   *  FunctionProperties object loaded from a properties string.
   * @throws IllegalFunctionException
   */
  void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException;

  /**
   * @return
   *  The minimum length of time needed to perform the anomaly detection function (null == all time). This can be
   *  interpreted as the size of the training set extending from the beginning of the detection interval.
   */
  TimeGranularity getTrainingWindowTimeGranularity();

  /**
   * @return
   *  The granularity of data requested by the function (cannot be null)
   */
  TimeGranularity getAggregationTimeGranularity();

  /**
   * @return
   *  The metrics used by this function.
   */
  Set<String> getMetrics();

  /**
   * Computes whether or not this dimension combination + metric time series contains anomalies in the
   * detection interval.
   *
   * @param dimensionKey
   *  The dimension combination associated with the series
   * @param series
   *  The time series to be analyzed including the training data
   * @param detectionInterval
   *  The interval of time to produce anomalies for
   * @param anomalyHistory
   *  A list of all previous anomaly results produced by this function
   *
   * @return
   *  The list of anomalies in the series. Anomaly results with isAnomaly set to false or not in the detectionInterval
   *  may be ignored by the driver.
   */
  List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries series, TimeRange detectionInterval,
      List<AnomalyResult> anomalyHistory);

}