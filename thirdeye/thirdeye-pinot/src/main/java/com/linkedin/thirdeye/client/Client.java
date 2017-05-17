package com.linkedin.thirdeye.client;

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * This class defines the config of a single client used in thirdeye
 * Eg: PinotThirdEyeClient
 */
public class Client {

  private String className;
  private Map<String, String> properties;


  public Client() {

  }
  public Client(String className, Map<String, String> properties) {
    this.className = className;
    this.properties = properties;
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

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

}
