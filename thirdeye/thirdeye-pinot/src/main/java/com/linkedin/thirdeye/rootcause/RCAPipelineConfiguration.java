package com.linkedin.thirdeye.rootcause;

import java.util.Map;

/**
 * Class to keep configs of each individual external rca pipeline
 * name: name for pipeline
 * className: class name containing implementation for this pipeline
 * properties: map of property name and value, which are required by this pipeline for instantiation
 */
public class RCAPipelineConfiguration {
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
