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
package com.linkedin.thirdeye.hadoop.replacement;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TopKDimensionSpec;
import com.linkedin.thirdeye.hadoop.ThirdEyeConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class contains the config needed by ReplacementPhase
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class ReplacementPhaseConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private String timeColumnName;
  private Set<String> topKDimensionNames;

  public ReplacementPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   */
  public ReplacementPhaseConfig(List<String> dimensionNames, List<String> metricNames,
      List<MetricType> metricTypes, String timeColumnName, Set<String> topKDimensionNames) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.topKDimensionNames = topKDimensionNames;
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

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public Set<String> getTopKDimensionNames() {
    return topKDimensionNames;
  }

  public static ReplacementPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

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

    String timeColumnName = config.getTime().getColumnName();

    Set<String> topKDimensionNames = new HashSet<>();
    for (TopKDimensionSpec topKDimensionSpec : config.getTopKRollup().getTopKDimensionSpec()) {
      topKDimensionNames.add(topKDimensionSpec.getDimensionName());
    }

    return new ReplacementPhaseConfig(dimensionNames, metricNames, metricTypes, timeColumnName, topKDimensionNames);
  }

}
