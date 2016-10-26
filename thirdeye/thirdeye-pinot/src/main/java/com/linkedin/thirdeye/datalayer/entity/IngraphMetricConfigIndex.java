package com.linkedin.thirdeye.datalayer.entity;

public class IngraphMetricConfigIndex extends AbstractIndexEntity {
  String metric;
  String metricAlias;
  String dataset;

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

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

}
