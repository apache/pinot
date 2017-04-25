package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderConfiguration;
import java.util.List;

/**
 * Config class for RCA's yml config
 * Maintain a list of configs for each external event data provider
 * Maintain a list of configs for each external pipeline using this config
 */
public class RCAConfiguration {

  private List<EventDataProviderConfiguration> eventDataProvidersConfiguration;
  private List<PipelineConfiguration> rcaPipelinesConfiguration;

  public List<EventDataProviderConfiguration> getEventDataProvidersConfiguration() {
    return eventDataProvidersConfiguration;
  }

  public void setEventDataProvidersConfiguration(
      List<EventDataProviderConfiguration> eventDataProvidersConfiguration) {
    this.eventDataProvidersConfiguration = eventDataProvidersConfiguration;
  }

  public List<PipelineConfiguration> getRcaPipelinesConfiguration() {
    return rcaPipelinesConfiguration;
  }

  public void setRcaPipelinesConfiguration(List<PipelineConfiguration> rcaPipelinesConfiguration) {
    this.rcaPipelinesConfiguration = rcaPipelinesConfiguration;
  }

}
