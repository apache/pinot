package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.List;

public class RollupPhaseTwoConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private List<String> rollupOrder;

  public RollupPhaseTwoConfig() {

  }

  /**
   * 
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   */
  public RollupPhaseTwoConfig(List<String> dimensionNames,
      List<String> metricNames, List<String> metricTypes,
      List<String> rollupOrder) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.rollupOrder = rollupOrder;
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

  public static RollupPhaseTwoConfig fromStarTreeConfig(StarTreeConfig config)
  {
    return new RollupPhaseTwoConfig(config.getDimensionNames(),
                                    config.getMetricNames(),
                                    config.getMetricTypes(),
                                    config.getRollup().getOrder());
  }
}
