package com.linkedin.thirdeye.bootstrap.rollup.phase1;

import java.util.List;

public class RollupPhaseOneConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private int rollupThreshold;

  public RollupPhaseOneConfig(){
    
  }
  /**
   * 
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param rollupThreshold
   */
  public RollupPhaseOneConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes, int rollupThreshold) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.rollupThreshold = rollupThreshold;
  }

  public int getRollupThreshold() {
    return rollupThreshold;
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

}
