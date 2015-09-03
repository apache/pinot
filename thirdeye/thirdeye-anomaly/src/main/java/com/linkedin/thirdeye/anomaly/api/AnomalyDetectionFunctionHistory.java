package com.linkedin.thirdeye.anomaly.api;

import java.util.List;

import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public interface AnomalyDetectionFunctionHistory {

  /**
   * @param queryTimeRange
   *  The range of time to get anomaly result history.
   */
  public void init(TimeRange queryTimeRange);

  /**
   * @param dimensionKey
   *  The combination of dimensions to query history.
   * @return
   *  The list of previously reported anomalies for the dimension key.
   */
  public List<AnomalyResult> getHistoryForDimensionKey(DimensionKey dimensionKey);

}
