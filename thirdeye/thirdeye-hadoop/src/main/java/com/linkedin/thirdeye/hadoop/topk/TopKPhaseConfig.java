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
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.TopKDimensionToMetricsSpec;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class contains the config needed by TopKPhase
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class TopKPhaseConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private Map<String, Double> metricThresholds;
  private Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec;
  private Map<String, Set<String>> whitelist;

  private static final double DEFAULT_METRIC_THRESHOLD = 0.01;
  private static final String FIELD_SEPARATOR = ",";

  public TopKPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param metricThresholds
   * @param whitelist
   */
  public TopKPhaseConfig(List<String> dimensionNames, List<String> metricNames, List<MetricType> metricTypes,
      Map<String, Double> metricThresholds, Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec, Map<String, Set<String>> whitelist) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.metricThresholds = metricThresholds;
    this.topKDimensionToMetricsSpec = topKDimensionToMetricsSpec;
    this.whitelist = whitelist;
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

  public Map<String, TopKDimensionToMetricsSpec> getTopKDimensionToMetricsSpec() {
    return topKDimensionToMetricsSpec;
  }

  public Map<String, Set<String>> getWhitelist() {
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
    List<String> metricNames = new ArrayList<String>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics()) {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    // dimensions
    List<String> dimensionNames = new ArrayList<String>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      dimensionNames.add(dimensionSpec.getName());
    }

    TopkWhitelistSpec topKWhitelist = config.getTopKWhitelist();
    Map<String, Double> metricThresholds = new HashMap<>();
    Map<String, TopKDimensionToMetricsSpec> topKDimensionToMetricsSpec = new HashMap<>();
    Map<String, Set<String>> whitelist = new HashMap<>();

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
        for (Entry<String, String> entry : topKWhitelist.getWhitelist().entrySet()) {
          String[] whitelistValues = entry.getValue().split(FIELD_SEPARATOR);
          whitelist.put(entry.getKey(), new HashSet<String>(Arrays.asList(whitelistValues)));
        }
      }
    }

    return new TopKPhaseConfig(dimensionNames, metricNames, metricTypes, metricThresholds, topKDimensionToMetricsSpec, whitelist);
  }


}
