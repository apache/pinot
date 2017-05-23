package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import java.util.List;
import org.joda.time.DateTime;


public class DummyAnomalyFunction extends BaseAnomalyFunction {
  public DummyAnomalyFunction(){}


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
  @Override
  public List<RawAnomalyResultDTO> analyze(DimensionMap exploredDimensions, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies) throws Exception {
    return null;
  }

  /**
   * Computes the score and severity according to the current and baseline of the given timeSeries and stores the
   * information to the merged anomaly. The start and end time of the time series is provided
   *
   * @param anomalyToUpdated
   *          the merged anomaly to be updated.
   * @param timeSeries
   *          The metric time series data.
   * @param windowStart

   *@param windowEnd

   * @param knownAnomalies
   *          Any known anomalies in the time range.  @return
   *         A list of anomalies that were not previously known.
   * @return the severity according to the current and baseline of the given timeSeries
   */
  @Override
  public void updateMergedAnomalyInfo(MergedAnomalyResultDTO anomalyToUpdated, MetricTimeSeries timeSeries,
      DateTime windowStart, DateTime windowEnd, List<MergedAnomalyResultDTO> knownAnomalies) throws Exception {

  }

  /**
   *
   * @return List of property keys applied in case of specific anomaly function
   */
  @Override
  public String[] getPropertyKeys() {
    return new String[0];
  }

}
