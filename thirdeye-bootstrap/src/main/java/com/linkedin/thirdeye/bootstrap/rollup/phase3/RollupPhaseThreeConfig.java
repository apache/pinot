package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import java.util.List;

public class RollupPhaseThreeConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private List<String> rollupOrder;
  private int rollupThreshold;

  public RollupPhaseThreeConfig() {

  }

  /**
   * 
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param rollupThreshold
   */
  public RollupPhaseThreeConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes,
      List<String> rollupOrder, int rollupThreshold) {
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
  public List<String> getRollupOrder() {
    return rollupOrder;
  }
}
