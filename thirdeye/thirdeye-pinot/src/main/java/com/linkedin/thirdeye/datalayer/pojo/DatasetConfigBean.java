package com.linkedin.thirdeye.datalayer.pojo;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.api.TimeSpec;

@JsonIgnoreProperties(ignoreUnknown=true)
public class DatasetConfigBean extends AbstractBean {

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

  private boolean active = true;

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

  public boolean isActive() {
    return active;
  }

  public void setActive(boolean active) {
    this.active = active;
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
        && Objects.equals(active, dc.isActive());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), dataset, dimensions, timeColumn, timeUnit, timeDuration, timeFormat, timezone,
        metricAsDimension, metricNamesColumn, metricValuesColumn, active);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("dataset", dataset)
        .add("dimensions", dimensions).add("dimensions", dimensions).add("timeUnit", timeUnit)
        .add("timeDuration", timeDuration).add("timeFormat", timeFormat).add("metricAsDimension", metricAsDimension)
        .add("metricNamesColumn", metricNamesColumn).add("metricValuesColumn", metricValuesColumn)
        .add("active", active).toString();
  }
}
