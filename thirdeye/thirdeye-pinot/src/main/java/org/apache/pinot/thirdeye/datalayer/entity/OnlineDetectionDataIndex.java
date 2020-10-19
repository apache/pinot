package org.apache.pinot.thirdeye.datalayer.entity;

public class OnlineDetectionDataIndex extends AbstractIndexEntity {
  String dataset;
  String metric;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }
}
