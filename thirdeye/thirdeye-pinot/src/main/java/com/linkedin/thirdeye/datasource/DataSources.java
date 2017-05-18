package com.linkedin.thirdeye.datasource;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This class keeps the datasource configs for all the datasources used in thirdeye
 */
public class DataSources {

  private List<DataSourceConfig> dataSourceConfigs = new ArrayList<>();

  public List<DataSourceConfig> getDataSourceConfigs() {
    return dataSourceConfigs;
  }
  public void setDataSourceConfigs(List<DataSourceConfig> dataSourceConfigs) {
    this.dataSourceConfigs = dataSourceConfigs;
  }


  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
