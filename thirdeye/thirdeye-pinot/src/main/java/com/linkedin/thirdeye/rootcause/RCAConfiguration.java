package com.linkedin.thirdeye.rootcause;

import java.util.List;

public class RCAConfiguration {

  private List<RCAPipelineConfiguration> rcaPipelinesConfiguration;

  public List<RCAPipelineConfiguration> getRcaPipelinesConfiguration() {
    return rcaPipelinesConfiguration;
  }

  public void setRcaPipelinesConfiguration(List<RCAPipelineConfiguration> rcaPipelinesConfiguration) {
    this.rcaPipelinesConfiguration = rcaPipelinesConfiguration;
  }


}
