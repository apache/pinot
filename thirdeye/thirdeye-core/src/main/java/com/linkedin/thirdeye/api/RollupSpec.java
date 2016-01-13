package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Properties;

public class RollupSpec {
  private String functionClass;
  private Properties functionConfig;
  private List<String> order;

  public RollupSpec() {
  }

  public RollupSpec(String functionClass, Properties functionConfig, List<String> order) {
    this.functionClass = functionClass;
    this.functionConfig = functionConfig;
    this.order = order;
  }

  @JsonProperty
  public String getFunctionClass() {
    return functionClass;
  }

  @JsonProperty
  public Properties getFunctionConfig() {
    return functionConfig;
  }

  @JsonProperty
  public List<String> getOrder() {
    return order;
  }
}
