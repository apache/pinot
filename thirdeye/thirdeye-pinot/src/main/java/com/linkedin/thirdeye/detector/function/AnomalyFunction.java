package com.linkedin.thirdeye.detector.function;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;

public interface AnomalyFunction {
  /** Initializes this function with its configuration, call before analyze */
  void init(AnomalyFunctionDTO spec) throws Exception;

  /** Returns the specification for this function instance */
  AnomalyFunctionDTO getSpec();

  /**
   * Analyzes a metric time series and returns any anomalous points / intervals.
   * @param exploredDimensions
   *          Pairs of dimension value and name corresponding to timeSeries.
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
  List<RawAnomalyResultDTO> analyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<RawAnomalyResultDTO> knownAnomalies)
      throws Exception;

  /**
   *
   * @return List of property keys applied in case of specific anomaly function
   */
  static String [] getPropertyKeys() {
    return new String[] {};
  }
}
