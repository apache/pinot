package com.linkedin.thirdeye.datasource.cache;

import java.util.Objects;

public class MetricDataset {

  private String metricName;
  private String dataset;

  public MetricDataset(String metricName, String dataset) {
    this.metricName = metricName;
    this.dataset = dataset;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(metricName, dataset);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MetricDataset)) {
      return false;
    }
    MetricDataset md = (MetricDataset) o;
    return Objects.equals(metricName, md.getMetricName())
        && Objects.equals(dataset, md.getDataset());
  }
}
