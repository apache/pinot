package com.linkedin.thirdeye.anomaly.events;

import java.util.Map;

/**
 * Class to keep configs of each individual external event data providers
 * name: name for event data provider
 * className: class name containing implementation for this event data provider
 * properties: map of property name and value, which are required by this event data provider for instantiation
 */
public class EventDataProviderConfiguration {
  private String name;
  private String className;
  private Map<String, String> properties = null;

  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
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



}
