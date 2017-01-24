package com.linkedin.thirdeye.datalayer.pojo;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.api.TimeSpec;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DatasetConfigBean extends AbstractBean {

  public static String DEFAULT_PREAGGREGATED_DIMENSION_VALUE = "all";
  public static String DATASET_OFFLINE_PREFIX = "_OFFLINE";

  private String dataset;

  private List<String> dimensions;

  private String timeColumn;

  private TimeUnit timeUnit;

  private Integer timeDuration;

  private String timeFormat = TimeSpec.SINCE_EPOCH_FORMAT;

  private String timezone = TimeSpec.DEFAULT_TIMEZONE;

  private boolean metricAsDimension = false;

  private String metricNamesColumn;

  private String metricValuesColumn;

  /** Autodiscover metrics in case of metricAsDimension */
  private boolean autoDiscoverMetrics = false;

  private boolean active = true;

  private boolean additive = true;
  private List<String> dimensionsHaveNoPreAggregation = Collections.emptyList();
  private String preAggregatedKeyword = DEFAULT_PREAGGREGATED_DIMENSION_VALUE;
  private Integer nonAdditiveBucketSize;
  private String nonAdditiveBucketUnit;

  private boolean realtime = false;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public String getTimeColumn() {
    return timeColumn;
  }

  public void setTimeColumn(String timeColumn) {
    this.timeColumn = timeColumn;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public void setTimeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
  }

  public Integer getTimeDuration() {
    return timeDuration;
  }

  public void setTimeDuration(Integer timeDuration) {
    this.timeDuration = timeDuration;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public void setTimeFormat(String timeFormat) {
    this.timeFormat = timeFormat;
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
  }

  public boolean isMetricAsDimension() {
    return metricAsDimension;
  }

  public void setMetricAsDimension(boolean metricAsDimension) {
    this.metricAsDimension = metricAsDimension;
  }

  public String getMetricNamesColumn() {
    return metricNamesColumn;
  }

  public void setMetricNamesColumn(String metricNamesColumn) {
    this.metricNamesColumn = metricNamesColumn;
  }

  public String getMetricValuesColumn() {
    return metricValuesColumn;
  }

  public void setMetricValuesColumn(String metricValuesColumn) {
    this.metricValuesColumn = metricValuesColumn;
  }


  public boolean isAutoDiscoverMetrics() {
    return autoDiscoverMetrics;
  }

  public void setAutoDiscoverMetrics(boolean autoDiscoverMetrics) {
    this.autoDiscoverMetrics = autoDiscoverMetrics;
  }

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  public boolean isAdditive() {
    return additive;
  }

  public void setAdditive(boolean additive) {
    this.additive = additive;
  }

  public List<String> getDimensionsHaveNoPreAggregation() {
    return dimensionsHaveNoPreAggregation;
  }

  public void setDimensionsHaveNoPreAggregation(List<String> dimensionsHaveNoPreAggregation) {
    this.dimensionsHaveNoPreAggregation = dimensionsHaveNoPreAggregation;
  }

  public String getPreAggregatedKeyword() {
    return preAggregatedKeyword;
  }

  public void setPreAggregatedKeyword(String preAggregatedKeyword) {
    this.preAggregatedKeyword = preAggregatedKeyword;
  }

  public Integer getNonAdditiveBucketSize() {
    return nonAdditiveBucketSize;
  }

  public void setNonAdditiveBucketSize(Integer nonAdditiveBucketSize) {
    this.nonAdditiveBucketSize = nonAdditiveBucketSize;
  }

  public String getNonAdditiveBucketUnit() {
    return nonAdditiveBucketUnit;
  }

  public void setNonAdditiveBucketUnit(String nonAdditiveBucketUnit) {
    this.nonAdditiveBucketUnit = nonAdditiveBucketUnit;
  }


  public boolean isRealtime() {
    return realtime;
  }

  public void setRealtime(boolean realtime) {
    this.realtime = realtime;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DatasetConfigBean)) {
      return false;
    }
    DatasetConfigBean dc = (DatasetConfigBean) o;
    return Objects.equals(getId(), dc.getId())
        && Objects.equals(dataset, dc.getDataset())
        && Objects.equals(dimensions, dc.getDimensions())
        && Objects.equals(timeColumn, dc.getTimeColumn())
        && Objects.equals(timeUnit, dc.getTimeUnit())
        && Objects.equals(timeDuration, dc.getTimeDuration())
        && Objects.equals(timeFormat, dc.getTimeFormat())
        && Objects.equals(timezone, dc.getTimezone())
        && Objects.equals(metricAsDimension, dc.isMetricAsDimension())
        && Objects.equals(metricNamesColumn, dc.getMetricNamesColumn())
        && Objects.equals(metricValuesColumn, dc.getMetricValuesColumn())
        && Objects.equals(autoDiscoverMetrics, dc.isAutoDiscoverMetrics())
        && Objects.equals(active, dc.isActive())
        && Objects.equals(additive, dc.isAdditive())
        && Objects.equals(dimensionsHaveNoPreAggregation, dc.getDimensionsHaveNoPreAggregation())
        && Objects.equals(preAggregatedKeyword, dc.getPreAggregatedKeyword())
        && Objects.equals(nonAdditiveBucketUnit, dc.getNonAdditiveBucketUnit())
        && Objects.equals(nonAdditiveBucketSize, dc.getNonAdditiveBucketSize())
        && Objects.equals(realtime, dc.isRealtime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, dimensions, timeColumn, timeUnit, timeDuration, timeFormat, timezone,
        metricAsDimension, metricNamesColumn, metricValuesColumn, autoDiscoverMetrics, active, additive,
        dimensionsHaveNoPreAggregation, preAggregatedKeyword, nonAdditiveBucketSize, nonAdditiveBucketUnit, realtime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("dataset", dataset)
        .add("dimensions", dimensions).add("timeUnit", timeUnit)
        .add("timeDuration", timeDuration).add("timeFormat", timeFormat).add("metricAsDimension", metricAsDimension)
        .add("metricNamesColumn", metricNamesColumn).add("metricValuesColumn", metricValuesColumn)
        .add("autoDiscoverMetrics", autoDiscoverMetrics).add("active", active).add("additive", additive)
        .add("dimensionsHaveNoPreAggregation", dimensionsHaveNoPreAggregation)
        .add("preAggregatedKeyword", preAggregatedKeyword).add("nonAdditiveBucketSize", nonAdditiveBucketSize)
        .add("nonAdditiveBucketUnit", nonAdditiveBucketUnit).add("offlineOnly", realtime).toString();
  }
}
