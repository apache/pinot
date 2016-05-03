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
package com.linkedin.thirdeye.hadoop.aggregation;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.hadoop.config.DimensionSpec;
import com.linkedin.thirdeye.hadoop.config.MetricSpec;
import com.linkedin.thirdeye.hadoop.config.MetricType;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.TimeSpec;

/**
 * This class contains the config needed by aggregation
 * and the methods to obtain the config from the ThirdEyeConfig
 */
public class AggregationPhaseConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private TimeSpec time;
  private TimeSpec inputTime;

  public AggregationPhaseConfig() {

  }

  public AggregationPhaseConfig(List<String> dimensionNames, List<String> metricNames,
      List<MetricType> metricTypes, TimeSpec time, TimeSpec inputTime) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.time = time;
    this.inputTime = inputTime;
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

  public TimeSpec getTime() {
    return time;
  }

  public TimeSpec getInputTime() {
    return inputTime;
  }

  public static AggregationPhaseConfig fromThirdEyeConfig(ThirdEyeConfig config) {

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
    TimeSpec time = config.getTime();

    // input time
    TimeSpec inputTime = config.getInputTime();
    if (inputTime == null) {
      throw new IllegalStateException("Must provide input time configs for aggregation job");
    }

    return new AggregationPhaseConfig(dimensionNames, metricNames, metricTypes, time, inputTime);
  }

}
