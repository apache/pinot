package com.linkedin.thirdeye.rootcause.impl;

import java.util.List;
import java.util.Map;


/**
 * Config class for RCA's yml config
 * Maintain a list of configs for each external event data provider
 * Maintain a list of configs for each external pipeline using this config
 */
public class RCAConfiguration {
  private Map<String, List<PipelineConfiguration>> frameworks;

  public Map<String, List<PipelineConfiguration>> getFrameworks() {
    return frameworks;
  }

  public void setFrameworks(Map<String, List<PipelineConfiguration>> frameworks) {
    this.frameworks = frameworks;
  }
}
