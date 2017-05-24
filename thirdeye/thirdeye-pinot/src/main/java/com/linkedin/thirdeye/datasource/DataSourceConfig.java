package com.linkedin.thirdeye.datasource;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This class defines the config of a single datasource used in thirdeye
 * Eg: PinotThirdeyeDataSource
 */
public class DataSourceConfig {

  private String className;
  private Map<String, String> properties;
  private String autoLoadClassName = null;


  public DataSourceConfig() {

  }
  public DataSourceConfig(String className, Map<String, String> properties, String autoLoadClassName) {
    this.className = className;
    this.properties = properties;
    this.autoLoadClassName = autoLoadClassName;
  }

  public String getClassName() {
    return className;
  }
  public void setClassName(String className) {
    this.className = className;
  }
  public Map<String, String> getProperties() {
    return properties;
  }
  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }
  public String getAutoLoadClassName() {
    return autoLoadClassName;
  }
  public void setAutoLoadClassName(String autoLoadClassName) {
    this.autoLoadClassName = autoLoadClassName;
  }
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
