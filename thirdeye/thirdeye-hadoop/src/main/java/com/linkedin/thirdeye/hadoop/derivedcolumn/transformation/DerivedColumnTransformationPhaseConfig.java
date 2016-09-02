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
package com.linkedin.thirdeye.hadoop.derivedcolumn.transformation;

import com.linkedin.thirdeye.hadoop.config.DimensionSpec;
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * This class contains the config needed by TopKColumnTransformation
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class DerivedColumnTransformationPhaseConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private String timeColumnName;
  private Map<String, Set<String>> whitelist;

  private static final String FIELD_SEPARATOR = ",";

  public DerivedColumnTransformationPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param timeColumnName
   * @param whitelist
   */
  public DerivedColumnTransformationPhaseConfig(List<String> dimensionNames, List<String> metricNames,
      List<MetricType> metricTypes, String timeColumnName, Map<String, Set<String>> whitelist) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
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

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public Map<String, Set<String>> getWhitelist() {
    return whitelist;
  }

  public static DerivedColumnTransformationPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

    // metrics
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

    // time
    String timeColumnName = config.getTime().getColumnName();

    TopkWhitelistSpec topKWhitelist = config.getTopKWhitelist();
    Map<String, Set<String>> whitelist = new HashMap<>();

    // topkwhitelist
    if (topKWhitelist != null && topKWhitelist.getWhitelist() != null) {
      for (Entry<String, String> entry : topKWhitelist.getWhitelist().entrySet()) {
        String[] whitelistValues = entry.getValue().split(FIELD_SEPARATOR);
        whitelist.put(entry.getKey(), new HashSet<String>(Arrays.asList(whitelistValues)));
      }
    }

    return new DerivedColumnTransformationPhaseConfig(dimensionNames, metricNames, metricTypes,
        timeColumnName, whitelist);
  }

}
