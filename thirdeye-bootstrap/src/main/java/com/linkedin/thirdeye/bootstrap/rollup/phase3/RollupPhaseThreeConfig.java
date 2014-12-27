package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.List;
import java.util.Map;

public class RollupPhaseThreeConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private List<String> rollupOrder;
  private String thresholdFuncClassName;
  private Map<String,String> thresholdFuncParams;
  
  public RollupPhaseThreeConfig() {

  }
/**
 * 
 * @param dimensionNames
 * @param metricNames
 * @param metricTypes
 * @param rollupOrder
 * @param thresholdFuncClassName
 * @param thresholdFuncParams
 */
  public RollupPhaseThreeConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes,
       String thresholdFuncClassName,
      Map<String, String> thresholdFuncParams) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.thresholdFuncClassName = thresholdFuncClassName;
    this.thresholdFuncParams = thresholdFuncParams;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<String> getMetricTypes() {
    return metricTypes;
  }

  public String getThresholdFuncClassName() {
    return thresholdFuncClassName;
  }

  public Map<String, String> getThresholdFuncParams() {
    return thresholdFuncParams;
  }

}
