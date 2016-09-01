package com.linkedin.thirdeye.datalayer.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.concurrent.TimeUnit;

public class AnomalyFunctionDTO extends AbstractDTO {

  private String collection;

  private String functionName;

  private String metric;

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
  public Multimap<String, String> getFilterSet() {
    return ThirdEyeUtils.getFilterSet(filters);
  }

  public void setFilters(String filters) {
    String sortedFilters = ThirdEyeUtils.getSortedFilters(filters);
    this.filters = sortedFilters;
  }
}
