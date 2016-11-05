package com.linkedin.thirdeye.detector.function;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
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
   * Returns the time ranges of data that is used by this anomaly function. This method is useful when multiple time
   * intervals are needed for fetching current vs baseline data
   *
   * @param monitoringWindowStartTime inclusive
   * @param monitoringWindowEndTime exclusive
   *
   * @return the time ranges of data that is used by this anomaly function
   */
   List<Pair<Long, Long>> getDataRangeIntervals(Long monitoringWindowStartTime, Long monitoringWindowEndTime);

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
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception;

  /**
   * Computes the score and severity according to the current and baseline of the given timeSeries and stores the
   * information to the merged anomaly. The start and end time of the time series is provided
   *
   * @param anomalyToUpdated
   *          the merged anomaly to be updated.
   * @param timeSeries
   *          The metric time series data.
   * @param knownAnomalies
   *          Any known anomalies in the time range.
   * @return
   *         A list of anomalies that were not previously known.
   * @return the severity according to the current and baseline of the given timeSeries
   */
  void updateMergedAnomalyInfo(MergedAnomalyResultDTO anomalyToUpdated, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies)
      throws Exception;

  /**
   *
   * @return List of property keys applied in case of specific anomaly function
   */
  static String [] getPropertyKeys() {
    return new String[] {};
  }
}
