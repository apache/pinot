package com.linkedin.thirdeye.rootcause.impl;

import java.util.Collection;
import java.util.Map;

/**
 * Class to keep configs of each individual external rca pipeline
 * name: name for pipeline
 * inputs: input names for pipeline
 * className: class name containing implementation for this pipeline
 * properties: map of property name and value, which are required by this pipeline for instantiation
 */
public class PipelineConfiguration {
  private String name;
  private String className;
  private Collection<String> inputs;
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
  public Collection<String> getInputs() {
    return inputs;
  }
  public void setInputs(Collection<String> inputs) {
    this.inputs = inputs;
  }
}
