package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

public class DashboardConfigDTO extends DashboardConfigBean {

  private DatasetConfigDTO datasetConfig;


  public DatasetConfigDTO getDatasetConfig() {
    return datasetConfig;
  }

  public void setDatasetConfig(DatasetConfigDTO datasetConfig) {
    this.datasetConfig = datasetConfig;
  }

}
