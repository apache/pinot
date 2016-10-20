package com.linkedin.thirdeye.datalayer.pojo;

public class IngraphMetricConfigBean extends AbstractBean {

  private String metric;

  private String metricAlias;

  private String fabrics;

  private String container;

  private String metricDataType;

  private String metricSourceType;

  private long numAvroRecords;

  private long startTimeInMs; // Inclusive

  private long endTimeInMs; // Inclusive

  private long intervalPeriod;

  private boolean bootstrap;

  private int granularitySize;

  private String granularityUnit;

  private String dataset;

  public IngraphMetricConfigBean() {
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getMetricAlias() {
    return metricAlias;
  }

  public void setMetricAlias(String metricAlias) {
    this.metricAlias = metricAlias;
  }

  public String getFabrics() {
    return fabrics;
  }

  public void setFabrics(String fabrics) {
    this.fabrics = fabrics;
  }

  public String getContainer() {
    return container;
  }

  public void setContainer(String container) {
    this.container = container;
  }

  public String getMetricDataType() {
    return metricDataType;
  }

  public void setMetricDataType(String metricDataType) {
    this.metricDataType = metricDataType;
  }

  public String getMetricSourceType() {
    return metricSourceType;
  }

  public void setMetricSourceType(String metricSourceType) {
    this.metricSourceType = metricSourceType;
  }

  public long getNumAvroRecords() {
    return numAvroRecords;
  }

  public void setNumAvroRecords(long numAvroRecords) {
    this.numAvroRecords = numAvroRecords;
  }

  public long getIntervalPeriod() {
    return intervalPeriod;
  }

  public void setIntervalPeriod(long intervalPeriod) {
    this.intervalPeriod = intervalPeriod;
  }

  public boolean isBootstrap() {
    return bootstrap;
  }

  public void setBootstrap(boolean bootstrap) {
    this.bootstrap = bootstrap;
  }

  public int getGranularitySize() {
    return granularitySize;
  }

  public void setGranularitySize(int granularitySize) {
    this.granularitySize = granularitySize;
  }

  public String getGranularityUnit() {
    return granularityUnit;
  }

  public void setGranularityUnit(String granularityUnit) {
    this.granularityUnit = granularityUnit;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  public void setStartTimeInMs(long startTimeInMs) {
    this.startTimeInMs = startTimeInMs;
  }

  public long getEndTimeInMs() {
    return endTimeInMs;
  }

  public void setEndTimeInMs(long endTimeInMs) {
    this.endTimeInMs = endTimeInMs;
  }

  @Override
  public String toString() {
    return "IngraphMetricConfigBean [metric=" + metric + ", metricAlias=" + metricAlias
        + ", fabrics=" + fabrics + ", container=" + container + ", metricType=" + metricDataType
        + ", inGraphMetricType=" + metricSourceType + ", numAvroRecords=" + numAvroRecords
        + ", start=" + startTimeInMs + ", end=" + endTimeInMs + ", intervalPeriod=" + intervalPeriod
        + ", bootstrap=" + bootstrap + ", granularitySize=" + granularitySize + ", granularityUnit="
        + granularityUnit + ", dataset=" + dataset + "]";
  }

}
