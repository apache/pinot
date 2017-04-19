package com.linkedin.thirdeye.rootcause;

import java.util.List;

/**
 * Config class for RCA's yml config
 * Maintain a list of configs for each external pipeline using this config
 */
public class RCAConfiguration {

  private List<RCAPipelineConfiguration> rcaPipelinesConfiguration;

  public List<RCAPipelineConfiguration> getRcaPipelinesConfiguration() {
    return rcaPipelinesConfiguration;
  }

  public void setRcaPipelinesConfiguration(List<RCAPipelineConfiguration> rcaPipelinesConfiguration) {
    this.rcaPipelinesConfiguration = rcaPipelinesConfiguration;
  }


}
