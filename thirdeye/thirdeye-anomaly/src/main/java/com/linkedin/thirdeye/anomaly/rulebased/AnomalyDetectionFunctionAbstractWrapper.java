package com.linkedin.thirdeye.anomaly.rulebased;

import java.util.Set;

import com.linkedin.thirdeye.anomaly.api.FunctionProperties;
import com.linkedin.thirdeye.anomaly.api.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.api.function.exception.IllegalFunctionException;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 *
 */
public abstract class AnomalyDetectionFunctionAbstractWrapper implements AnomalyDetectionFunction {

  protected AnomalyDetectionFunction childFunc;

  /**
   * @param childFunc
   * @throws IllegalFunctionException
   */
  public AnomalyDetectionFunctionAbstractWrapper(AnomalyDetectionFunction childFunc) throws IllegalFunctionException {
    if (childFunc == null) {
      throw new IllegalFunctionException("nested child function cannot be null");
    }
    this.childFunc = childFunc;
  }

  @Override
  public void init(StarTreeConfig starTreeConfig, FunctionProperties functionConfig) throws IllegalFunctionException {
    childFunc.init(starTreeConfig, functionConfig);
  }

  @Override
  public TimeGranularity getAggregationTimeGranularity() {
    return childFunc.getAggregationTimeGranularity();
  }

  @Override
  public Set<String> getMetrics() {
    return childFunc.getMetrics();
  }

  @Override
  public TimeGranularity getMinimumMonitoringIntervalTimeGranularity() {
    return null;
  }
}
