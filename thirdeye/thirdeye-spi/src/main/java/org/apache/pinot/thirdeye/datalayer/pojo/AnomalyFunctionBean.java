/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.pinot.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.anomaly.merge.AnomalyMergeConfig;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


@JsonIgnoreProperties(ignoreUnknown=true)
public class AnomalyFunctionBean extends AbstractBean {

  private String collection;

  private String functionName;

  // This variable is topic metric, which is used as a search criteria for searching anomalies
  private String metric;

  // The list of metrics that have to be retrieved in order to detect anomalies for this function
  private List<String> metrics;

  private MetricAggFunction metricFunction;

  private String type;
  private List<String> secondaryAnomalyFunctionsType;

  private boolean isActive = true;

  /**
   * Define the metric and the filters of global metric
   * TODO: 1) Define a metric node with the metric and filter information, and 2) Point the global metric to the id of the metric node.
   *
   */
  private String globalMetric;

  private String globalMetricFilters;

  private String properties;

  private String cron;

  private TimeGranularity frequency = new TimeGranularity(1, TimeUnit.HOURS);

  private Integer bucketSize;

  private TimeUnit bucketUnit;

  private Integer windowSize;

  private TimeUnit windowUnit;

  private Integer windowDelay;

  private TimeUnit windowDelayUnit;

  private String exploreDimensions;

  private String filters;

  private long metricId;

  // Used to remove time series with small traffic or total count if the information is available
  // This filter is different from the calculation of OTHER dimensions because it removes the time series if the
  // count is not satisfied. Moreover, we won't have top level traffic or total count information when this filter
  // is invoked, i.e., this filter is a local filter to time series with a certain dimensions.
  private Map<String, String> dataFilter;

  private Map<String, String> alertFilter;

  private AnomalyMergeConfig anomalyMergeConfig;

  /**
   * This flag always true.
   * This flag would typically be unset, in backfill cases, where we want to override the completeness check,
   */
  private boolean requiresCompletenessCheck = true;

  public List<String> getSecondaryAnomalyFunctionsType() {
    return secondaryAnomalyFunctionsType;
  }

  public void setSecondaryAnomalyFunctionsType(List<String> secondaryAnomalyFunctionsType) {
    this.secondaryAnomalyFunctionsType = secondaryAnomalyFunctionsType;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getFunctionName() {
    return functionName;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<String> metrics) {
    this.metrics = metrics;
  }

  public MetricAggFunction getMetricFunction() {
    return metricFunction;
  }

  public void setMetricFunction(MetricAggFunction metricFunction) {
    this.metricFunction = metricFunction;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean getIsActive() {
    return isActive;
  }

  public void setIsActive(boolean isActive) {
    this.isActive = isActive;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getCron() {
    return cron;
  }

  public void setCron(String cron) {
    this.cron = cron;
  }

  public String getGlobalMetric() {
    return globalMetric;
  }

  public void setGlobalMetric(String globalMetric) {
    this.globalMetric = globalMetric;
  }

  public String getGlobalMetricFilters() {
    return globalMetricFilters;
  }

  public void setGlobalMetricFilters(String globalMetricFilters) {
    this.globalMetricFilters = globalMetricFilters;
  }

  public TimeGranularity getFrequency() {
    return frequency;
  }

  public void setFrequency(TimeGranularity frequency) {
    this.frequency = frequency;
  }

  public Integer getBucketSize() {
    return bucketSize;
  }

  public void setBucketSize(Integer bucketSize) {
    this.bucketSize = bucketSize;
  }

  public TimeUnit getBucketUnit() {
    return bucketUnit;
  }

  public void setBucketUnit(TimeUnit bucketUnit) {
    this.bucketUnit = bucketUnit;
  }

  public Integer getWindowSize() {
    return windowSize;
  }

  public void setWindowSize(int windowSize) {
    this.windowSize = windowSize;
  }

  public TimeUnit getWindowUnit() {
    return windowUnit;
  }

  public void setWindowUnit(TimeUnit windowUnit) {
    this.windowUnit = windowUnit;
  }

  public Integer getWindowDelay() {
    return windowDelay;
  }

  public void setWindowDelay(int windowDelay) {
    this.windowDelay = windowDelay;
  }

  public TimeUnit getWindowDelayUnit() {
    return windowDelayUnit;
  }

  public void setWindowDelayUnit(TimeUnit windowDelayUnit) {
    this.windowDelayUnit = windowDelayUnit;
  }

  public String getExploreDimensions() {
    return exploreDimensions;
  }

  public void setExploreDimensions(String exploreDimensions) {
    this.exploreDimensions = exploreDimensions;
  }

  public String getFilters() {
    return filters;
  }

  public boolean isRequiresCompletenessCheck() {
    return requiresCompletenessCheck;
  }

  public void setRequiresCompletenessCheck(boolean requiresCompletenessCheck) {
    this.requiresCompletenessCheck = requiresCompletenessCheck;
  }

  @JsonIgnore
  @JsonProperty("wrapper")
  public Multimap<String, String> getFilterSet() {
    return ThirdEyeUtils.getFilterSet(filters);
  }

  @JsonIgnore
  @JsonProperty("wrapper")
  public void setFilters(String filters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    this.filters = sortedFilters;
  }

  public void setActive(boolean isActive) {
    this.isActive = isActive;
  }

  public Map<String, String> getAlertFilter() {
    return alertFilter;
  }

  public void setAlertFilter(Map<String, String> alertFilter) {
    this.alertFilter = alertFilter;
  }

  public Map<String, String> getDataFilter() {
    return dataFilter;
  }

  public void setDataFilter(Map<String, String> dataFilter) {
    this.dataFilter = dataFilter;
  }

  public AnomalyMergeConfig getAnomalyMergeConfig() {
    return anomalyMergeConfig;
  }

  public void setAnomalyMergeConfig(AnomalyMergeConfig anomalyMergeConfig) {
    this.anomalyMergeConfig = anomalyMergeConfig;
  }

  public void setWindowSize(Integer windowSize) {
    this.windowSize = windowSize;
  }

  public void setWindowDelay(Integer windowDelay) {
    this.windowDelay = windowDelay;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AnomalyFunctionBean that = (AnomalyFunctionBean) o;
    return isActive == that.isActive && metricId == that.metricId
        && requiresCompletenessCheck == that.requiresCompletenessCheck && Objects.equals(collection, that.collection)
        && Objects.equals(functionName, that.functionName) && Objects.equals(metric, that.metric) && Objects.equals(
        metrics, that.metrics) && metricFunction == that.metricFunction && Objects.equals(type, that.type)
        && Objects.equals(globalMetric, that.globalMetric) && Objects.equals(globalMetricFilters,
        that.globalMetricFilters) && Objects.equals(properties, that.properties) && Objects.equals(cron, that.cron)
        && Objects.equals(frequency, that.frequency) && Objects.equals(bucketSize, that.bucketSize)
        && bucketUnit == that.bucketUnit && Objects.equals(windowSize, that.windowSize) && windowUnit == that.windowUnit
        && Objects.equals(windowDelay, that.windowDelay) && windowDelayUnit == that.windowDelayUnit && Objects.equals(
        exploreDimensions, that.exploreDimensions) && Objects.equals(filters, that.filters) && Objects.equals(
        dataFilter, that.dataFilter) && Objects.equals(alertFilter, that.alertFilter) && Objects.equals(
        anomalyMergeConfig, that.anomalyMergeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collection, functionName, metric, metrics, metricFunction, type, isActive, globalMetric,
        globalMetricFilters, properties, cron, frequency, bucketSize, bucketUnit, windowSize, windowUnit, windowDelay,
        windowDelayUnit, exploreDimensions, filters, metricId, dataFilter, alertFilter, anomalyMergeConfig,
        requiresCompletenessCheck);
  }
}
