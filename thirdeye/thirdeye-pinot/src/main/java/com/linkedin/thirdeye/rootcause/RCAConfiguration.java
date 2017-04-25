package com.linkedin.thirdeye.rootcause;

import java.util.List;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderConfiguration;

/**
 * Config class for RCA's yml config
 * Maintain a list of configs for each external event data provider
 * Maintain a list of configs for each external pipeline using this config
 */
public class RCAConfiguration {

  private List<EventDataProviderConfiguration> eventDataProvidersConfiguration;
  private List<RCAPipelineConfiguration> rcaPipelinesConfiguration;

  public List<EventDataProviderConfiguration> getEventDataProvidersConfiguration() {
    return eventDataProvidersConfiguration;
  }

  public void setEventDataProvidersConfiguration(
      List<EventDataProviderConfiguration> eventDataProvidersConfiguration) {
    this.eventDataProvidersConfiguration = eventDataProvidersConfiguration;
  }

  public List<RCAPipelineConfiguration> getRcaPipelinesConfiguration() {
    return rcaPipelinesConfiguration;
  }

  public void setRcaPipelinesConfiguration(List<RCAPipelineConfiguration> rcaPipelinesConfiguration) {
    this.rcaPipelinesConfiguration = rcaPipelinesConfiguration;
  }

}
