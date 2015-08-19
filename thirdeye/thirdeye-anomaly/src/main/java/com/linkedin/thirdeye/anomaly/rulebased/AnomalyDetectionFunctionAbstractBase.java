package com.linkedin.thirdeye.anomaly.rulebased;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.external.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 *
 */
public abstract class AnomalyDetectionFunctionAbstractBase implements AnomalyDetectionFunction {

  private TimeGranularity aggregateTimeGranularity;

  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    // nothing to do
  }

  public AnomalyDetectionFunctionAbstractBase(TimeGranularity aggregateTimeGranularity) {
    super();
    this.aggregateTimeGranularity = aggregateTimeGranularity;
  }

  @Override
  public TimeGranularity getAggregationTimeGranularity() {
    return aggregateTimeGranularity;
  }
}
