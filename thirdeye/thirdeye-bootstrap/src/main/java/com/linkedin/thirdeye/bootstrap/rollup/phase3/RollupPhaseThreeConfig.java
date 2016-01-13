package com.linkedin.thirdeye.bootstrap.rollup.phase3;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RollupPhaseThreeConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private List<String> rollupOrder;
  private String thresholdFuncClassName;
  private Map<String, String> thresholdFuncParams;

  public RollupPhaseThreeConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param thresholdFuncClassName
   * @param thresholdFuncParams
   */
  public RollupPhaseThreeConfig(List<String> dimensionNames, List<String> metricNames,
      List<MetricType> metricTypes, String thresholdFuncClassName,
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

  public List<MetricType> getMetricTypes() {
    return metricTypes;
  }

  public String getThresholdFuncClassName() {
    return thresholdFuncClassName;
  }

  public Map<String, String> getThresholdFuncParams() {
    return thresholdFuncParams;
  }

  public static RollupPhaseThreeConfig fromStarTreeConfig(StarTreeConfig config) {
    Map<String, String> rollupFunctionConfig = new HashMap<String, String>();

    if (config.getRollup().getFunctionConfig() != null) {
      for (Map.Entry<Object, Object> entry : config.getRollup().getFunctionConfig().entrySet()) {
        rollupFunctionConfig.put((String) entry.getKey(), (String) entry.getValue());
      }
    }

    List<String> metricNames = new ArrayList<String>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics()) {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    List<String> dimensionNames = new ArrayList<String>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      dimensionNames.add(dimensionSpec.getName());
    }

    return new RollupPhaseThreeConfig(dimensionNames, metricNames, metricTypes,
        config.getRollup().getFunctionClass(), rollupFunctionConfig);
  }
}
