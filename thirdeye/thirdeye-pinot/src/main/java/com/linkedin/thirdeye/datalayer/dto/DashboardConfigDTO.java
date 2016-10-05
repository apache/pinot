package com.linkedin.thirdeye.datalayer.dto;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

public class DashboardConfigDTO extends DashboardConfigBean {

  private List<MetricConfigDTO> metrics = new ArrayList<>();
  private DatasetConfigDTO datasetConfig;

  public List<MetricConfigDTO> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<MetricConfigDTO> metrics) {
    this.metrics = metrics;
  }

  public DatasetConfigDTO getDatasetConfig() {
    return datasetConfig;
  }

  public void setDatasetConfig(DatasetConfigDTO datasetConfig) {
    this.datasetConfig = datasetConfig;
  }

}
