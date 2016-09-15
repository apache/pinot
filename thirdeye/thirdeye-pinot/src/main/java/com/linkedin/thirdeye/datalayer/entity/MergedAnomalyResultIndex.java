package com.linkedin.thirdeye.datalayer.entity;

public class MergedAnomalyResultIndex extends AbstractIndexEntity {


  long functionId;
  long anomalyFeedbackId;
  long metricId;
  long startTime;
  long endTime;
  String collection;
  String metric;
  String dimensions;
  boolean notified;

  public long getAnomalyFeedbackId() {
    return anomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(long anomalyFeedbackId) {
    this.anomalyFeedbackId = anomalyFeedbackId;
  }

  public long getFunctionId() {
    return functionId;
  }

  public void setFunctionId(long anomalyFunctionId) {
    this.functionId = anomalyFunctionId;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public long getMetricId() {
    return metricId;
  }

  public void setMetricId(long metricId) {
    this.metricId = metricId;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public boolean isNotified() {
    return notified;
  }

  public void setNotified(boolean notified) {
    this.notified = notified;
  }

  @Override
  public String toString() {
    return "MergedAnomalyResultIndex [functionId=" + functionId + ", anomalyFeedbackId="
        + anomalyFeedbackId + ", metricId=" + metricId + ", startTime=" + startTime + ", endTime="
        + endTime + ", collection=" + collection + ", metric=" + metric + ", dimensionValue="
        + dimensions + ", notified=" + notified + ", baseId=" + baseId + ", id=" + id
        + ", createTime=" + createTime + ", updateTime=" + updateTime + ", version=" + version
        + "]";
  }
}
