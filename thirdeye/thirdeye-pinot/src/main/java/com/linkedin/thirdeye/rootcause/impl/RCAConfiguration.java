package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderConfiguration;
import java.util.List;
import java.util.Map;


/**
 * Config class for RCA's yml config
 * Maintain a list of configs for each external event data provider
 * Maintain a list of configs for each external pipeline using this config
 */
public class RCAConfiguration {

  private List<EventDataProviderConfiguration> eventDataProviders;
  private Map<String, List<PipelineConfiguration>> frameworks;

  public List<EventDataProviderConfiguration> getEventDataProviders() {
    return eventDataProviders;
  }

  public void setEventDataProviders(
      List<EventDataProviderConfiguration> eventDataProviders) {
    this.eventDataProviders = eventDataProviders;
  }

  public Map<String, List<PipelineConfiguration>> getFrameworks() {
    return frameworks;
  }

  public void setFrameworks(Map<String, List<PipelineConfiguration>> frameworks) {
    this.frameworks = frameworks;
  }

}
