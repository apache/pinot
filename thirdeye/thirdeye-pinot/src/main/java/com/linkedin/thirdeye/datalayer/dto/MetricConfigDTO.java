package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.MetricConfigBean;
import com.wordnik.swagger.annotations.ApiModel;


public class MetricConfigDTO extends MetricConfigBean {

  private DatasetConfigDTO datasetConfig;

  public DatasetConfigDTO getDatasetConfig() {
    return datasetConfig;
  }

  public void setDatasetConfig(DatasetConfigDTO datasetConfig) {
    this.datasetConfig = datasetConfig;
  }

}
