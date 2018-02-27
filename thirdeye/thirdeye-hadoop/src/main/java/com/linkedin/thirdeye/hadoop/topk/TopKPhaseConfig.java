/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.topk;

import com.linkedin.thirdeye.hadoop.config.DimensionSpec;
import com.linkedin.thirdeye.hadoop.config.DimensionType;
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;

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
  private List<DimensionType> dimensionTypes;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private Map<String, Double> metricThresholds;
  private Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec;
  private Map<String, List<String>> whitelist;

  private static final double DEFAULT_METRIC_THRESHOLD = 0.01;

  public TopKPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param dimensionTypes
   * @param metricNames
   * @param metricTypes
   * @param metricThresholds
   * @param whitelist
   */
  public TopKPhaseConfig(List<String> dimensionNames, List<DimensionType> dimensionTypes,
      List<String> metricNames, List<MetricType> metricTypes,
      Map<String, Double> metricThresholds, Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec,
      Map<String, List<String>> whitelist) {
    super();
    this.dimensionNames = dimensionNames;
    this.dimensionTypes = dimensionTypes;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.metricThresholds = metricThresholds;
    this.topKDimensionToMetricsSpec = topKDimensionToMetricsSpec;
    this.whitelist = whitelist;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<DimensionType> getDimensionTypes() {
    return dimensionTypes;
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

  public Map<String, TopKDimensionToMetricsSpec> getTopKDimensionToMetricsSpec() {
    return topKDimensionToMetricsSpec;
  }

  public Map<String, List<String>> getWhitelist() {
    return whitelist;
  }

  /**
   * This method generates necessary top k config for TopKPhase job from
   * ThirdEye config
   * @param config
   * @return
   */
  public static TopKPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

    //metrics
    List<String> metricNames = new ArrayList<>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics()) {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    // dimensions
    List<String> dimensionNames = new ArrayList<>(config.getDimensions().size());
    List<DimensionType> dimensionTypes = new ArrayList<>(config.getDimensions().size());
    for (DimensionSpec spec : config.getDimensions()) {
      dimensionNames.add(spec.getName());
      dimensionTypes.add(spec.getDimensionType());
    }

    TopkWhitelistSpec topKWhitelist = config.getTopKWhitelist();
    Map<String, Double> metricThresholds = new HashMap<>();
    Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec = new HashMap<>();
    Map<String, List<String>> whitelist = new HashMap<>();

    // topk
    if (topKWhitelist != null) {
      // metric thresholds
      if (topKWhitelist.getThreshold() != null) {
        metricThresholds = topKWhitelist.getThreshold();
      }
      for (String metric : metricNames) {
        if (metricThresholds.get(metric) == null) {
          metricThresholds.put(metric, DEFAULT_METRIC_THRESHOLD);
        }
      }

      // topk
      if (topKWhitelist.getTopKDimensionToMetricsSpec() != null) {
        for (TopKDimensionToMetricsSpec topkSpec : topKWhitelist.getTopKDimensionToMetricsSpec()) {
          topKDimensionToMetricsSpec.put(topkSpec.getDimensionName(), topkSpec);
        }
      }

      // whitelist
      if (topKWhitelist.getWhitelist() != null) {
        whitelist.putAll(topKWhitelist.getWhitelist());
      }
    }

    return new TopKPhaseConfig(dimensionNames, dimensionTypes, metricNames, metricTypes, metricThresholds,
        topKDimensionToMetricsSpec, whitelist);
  }


}
