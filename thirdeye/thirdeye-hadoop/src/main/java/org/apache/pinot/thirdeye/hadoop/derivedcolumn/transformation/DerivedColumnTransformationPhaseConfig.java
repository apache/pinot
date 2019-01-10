/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.thirdeye.hadoop.config.DimensionType;
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.TopkWhitelistSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class contains the config needed by TopKColumnTransformation
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class DerivedColumnTransformationPhaseConfig {
  private List<String> dimensionNames;
  private List<DimensionType> dimensionTypes;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private String timeColumnName;
  private Map<String, List<String>> whitelist;
  private Map<String, String> nonWhitelistValue;


  public DerivedColumnTransformationPhaseConfig() {

  }

  /**
   * @param dimensionNames
   * @param dimensionTypes
   * @param metricNames
   * @param metricTypes
   * @param timeColumnName
   * @param whitelist
   */
  public DerivedColumnTransformationPhaseConfig(List<String> dimensionNames, List<DimensionType> dimensionTypes,
      List<String> metricNames, List<MetricType> metricTypes, String timeColumnName,
      Map<String, List<String>> whitelist, Map<String, String> nonWhitelistValue) {
    super();
    this.dimensionNames = dimensionNames;
    this.dimensionTypes = dimensionTypes;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.whitelist = whitelist;
    this.nonWhitelistValue = nonWhitelistValue;
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

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public Map<String, List<String>> getWhitelist() {
    return whitelist;
  }

  public Map<String, String> getNonWhitelistValue() {
    return nonWhitelistValue;
  }

  public static DerivedColumnTransformationPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

    // metrics
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

    // time
    String timeColumnName = config.getTime().getColumnName();

    TopkWhitelistSpec topKWhitelist = config.getTopKWhitelist();
    Map<String, List<String>> whitelist = new HashMap<>();

    // topkwhitelist
    if (topKWhitelist != null && topKWhitelist.getWhitelist() != null) {
      whitelist.putAll(topKWhitelist.getWhitelist());
    }

    Map<String, String> nonWhitelistValueMap = new HashMap<>();
    if (topKWhitelist != null && topKWhitelist.getNonWhitelistValue() != null) {
      nonWhitelistValueMap.putAll(topKWhitelist.getNonWhitelistValue());
    }

    return new DerivedColumnTransformationPhaseConfig(dimensionNames, dimensionTypes, metricNames, metricTypes,
        timeColumnName, whitelist, nonWhitelistValueMap);
  }

}
