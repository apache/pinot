package com.linkedin.thirdeye.datalayer.entity;

public class OnboardDatasetMetricIndex extends AbstractIndexEntity {
  String datasetName;
  String metricName;
  String dataSource;
  boolean onboarded;

  public String getDatasetName() {
    return datasetName;
  }
  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }
  public String getMetricName() {
    return metricName;
  }
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
  public String getDataSource() {
    return dataSource;
  }
  public void setDataSource(String dataSource) {
    this.dataSource = dataSource;
  }
  public boolean isOnboarded() {
    return onboarded;
  }
  public void setOnboarded(boolean onboarded) {
    this.onboarded = onboarded;
  }

}
