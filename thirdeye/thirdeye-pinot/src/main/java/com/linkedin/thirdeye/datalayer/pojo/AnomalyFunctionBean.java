package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


@JsonIgnoreProperties(ignoreUnknown=true)
public class AnomalyFunctionBean extends AbstractBean {

  private String collection;

  private String functionName;

  // This variable is topic metric
  private String metric;

  private List<String> metrics;

  private MetricAggFunction metricFunction;

  private String type;

  private boolean isActive = true;

  private String properties;

  private String cron;

  private Integer bucketSize;

  private TimeUnit bucketUnit;

  private Integer windowSize;

  private TimeUnit windowUnit;

  private Integer windowDelay;

  private TimeUnit windowDelayUnit;

  private String exploreDimensions;

  private String filters;

  private long metricId;

  private Map<String, String> alertFilter;

  private AnomalyMergeConfig anomalyMergeConfig;


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

  public AnomalyMergeConfig getAnomalyMergeConfig() {
    return anomalyMergeConfig;
  }

  public void setAnomalyMergeConfig(AnomalyMergeConfig anomalyMergeConfig) {
    this.anomalyMergeConfig = anomalyMergeConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyFunctionBean)) {
      return false;
    }
    AnomalyFunctionBean af = (AnomalyFunctionBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(collection, af.getCollection())
        && Objects.equals(metric, af.getMetric())
        && Objects.equals(metrics, af.getMetrics())
        && Objects.equals(metricFunction, af.getMetricFunction())
        && Objects.equals(type, af.getType()) && Objects.equals(isActive, af.getIsActive())
        && Objects.equals(cron, af.getCron()) && Objects.equals(properties, af.getProperties())
        && Objects.equals(bucketSize, af.getBucketSize())
        && Objects.equals(bucketUnit, af.getBucketUnit())
        && Objects.equals(windowSize, af.getWindowSize())
        && Objects.equals(windowUnit, af.getWindowUnit())
        && Objects.equals(windowDelay, af.getWindowDelay())
        && Objects.equals(windowDelayUnit, af.getWindowDelayUnit())
        && Objects.equals(exploreDimensions, af.getExploreDimensions())
        && Objects.equals(filters, af.getFilters())
        && Objects.equals(alertFilter, af.getAlertFilter());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), collection, metric, metrics, metricFunction, type, isActive, cron,
        properties, bucketSize, bucketUnit, windowSize, windowUnit, windowDelay, windowDelayUnit,
        exploreDimensions, filters, alertFilter);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("collection", collection)
        .add("metric", metric).add("metrics", metrics).add("metric_function", getMetricFunction()).add("type", type)
        .add("isActive", isActive).add("cron", cron).add("properties", properties)
        .add("bucketSize", bucketSize).add("bucketUnit", bucketUnit).add("windowSize", windowSize)
        .add("windowUnit", windowUnit).add("windowDelay", windowDelay)
        .add("windowDelayUnit", windowDelayUnit).add("exploreDimensions", exploreDimensions)
        .add("filters", filters).add("alertFilter", alertFilter).toString();
  }
}
