package com.linkedin.thirdeye.bootstrap.topkrollup.phase3;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TopKDimensionSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopKRollupPhaseThreeConfig {

  private static Logger LOGGER = LoggerFactory.getLogger(TopKRollupPhaseThreeConfig.class);

  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;

  /**
   *
   */
  public TopKRollupPhaseThreeConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   */
  public TopKRollupPhaseThreeConfig(List<String> dimensionNames, List<String> metricNames,
      List<MetricType> metricTypes) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
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

  public static TopKRollupPhaseThreeConfig fromStarTreeConfig(StarTreeConfig config) {

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

    return new TopKRollupPhaseThreeConfig(dimensionNames, metricNames, metricTypes);
  }
}
