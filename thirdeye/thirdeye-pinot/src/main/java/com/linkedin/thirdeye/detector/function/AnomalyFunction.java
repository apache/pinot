package com.linkedin.thirdeye.detector.function;

import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.detector.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.db.entity.AnomalyResult;

public interface AnomalyFunction {
  /** Initializes this function with its configuration, call before analyze */
  void init(AnomalyFunctionSpec spec) throws Exception;

  /** Returns the specification for this function instance */
  AnomalyFunctionSpec getSpec();

  /**
   * Analyzes a metric time series and returns any anomalous points / intervals.
   * @param dimensionKey
   *          The dimension combination corresponding to timeSeries.
   * @param timeSeries
   *          The metric time series data.
   * @param windowStart
   *          The beginning of the range corresponding to timeSeries.
   * @param windowEnd
   *          The end of the range corresponding to timeSeries.
   * @param knownAnomalies
   *          Any known anomalies in the time range.
   * @return
   *         A list of anomalies that were not previously known.
   */
  List<AnomalyResult> analyze(DimensionKey dimensionKey, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<AnomalyResult> knownAnomalies)
      throws Exception;

  /**
   *
   * @return List of property keys applied in case of specific anomaly function
   */
  static String [] getPropertyKeys() {
    return new String[] {};
  }
}
