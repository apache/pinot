package com.linkedin.thirdeye.anomaly.api;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class AnomalyDetectionFunctionHistoryNoOp implements AnomalyDetectionFunctionHistory {

  private static final AnomalyDetectionFunctionHistoryNoOp _sharedInstance = new AnomalyDetectionFunctionHistoryNoOp();

  public static AnomalyDetectionFunctionHistoryNoOp sharedInstance() {
    return _sharedInstance;
  }

  private final List<AnomalyResult> emptyList = new ImmutableList.Builder<AnomalyResult>().build();

  private AnomalyDetectionFunctionHistoryNoOp() {
    // Do nothing
  }
  @Override
  public void init(TimeRange queryTimeRange) {
    // Do nothing
  }

  @Override
  public List<AnomalyResult> getHistoryForDimensionKey(DimensionKey dimensionKey) {
    return emptyList;
  }

}
