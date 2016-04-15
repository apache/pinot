package com.linkedin.thirdeye.hadoop.topk;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TopKDimensionSpec;
import com.linkedin.thirdeye.hadoop.ThirdEyeConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the config needed by TopKPhase
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class TopKPhaseConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private Map<String, Double> metricThresholds;
  private Map<String, TopKDimensionSpec> topKDimensionSpec;

  private static double DEFAULT_METRIC_THRESHOLD = 0.01;

  public TopKPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param metricThresholds
   */
  public TopKPhaseConfig(List<String> dimensionNames, List<String> metricNames, List<MetricType> metricTypes,
      Map<String, Double> metricThresholds, Map<String, TopKDimensionSpec> topKDimensionSpec) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.metricThresholds = metricThresholds;
    this.topKDimensionSpec = topKDimensionSpec;
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

  public Map<String, Double> getMetricThresholds() {
    return metricThresholds;
  }

  public Map<String, TopKDimensionSpec> getTopKDimensionSpec() {
    return topKDimensionSpec;
  }

  /**
   * This method generates necessary top k config for TopKPhase job from
   * ThirdEye config
   * @param config
   * @return
   */
  public static TopKPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

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

    Map<String, Double> metricThresholds = new HashMap<>();
    if (config.getTopKRollup() != null && config.getTopKRollup().getThreshold() != null) {
      metricThresholds = config.getTopKRollup().getThreshold();
    }
    for (String metric : metricNames) {
      if (metricThresholds.get(metric) == null) {
        metricThresholds.put(metric, DEFAULT_METRIC_THRESHOLD);
      }
    }

    Map<String, TopKDimensionSpec> topKDimensionSpec = new HashMap<>();
    if (config.getTopKRollup() != null && config.getTopKRollup().getTopKDimensionSpec() != null) {
      for (TopKDimensionSpec topkSpec : config.getTopKRollup().getTopKDimensionSpec()) {
        topKDimensionSpec.put(topkSpec.getDimensionName(), topkSpec);
      }
    }

    return new TopKPhaseConfig(dimensionNames, metricNames, metricTypes, metricThresholds, topKDimensionSpec);
  }


}
