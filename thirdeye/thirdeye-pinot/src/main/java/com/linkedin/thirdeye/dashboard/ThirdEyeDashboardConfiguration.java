package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.configs.ResourceConfiguration;
import java.util.List;

public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  RootCauseConfiguration rootCause;
  List<ResourceConfiguration> resourceConfig;

  public List<ResourceConfiguration> getResourceConfig() {
    return resourceConfig;
  }

  public void setResourceConfig(List<ResourceConfiguration> resourceConfig) {
    this.resourceConfig = resourceConfig;
  }

  public RootCauseConfiguration getRootCause() {
    return rootCause;
  }

  public void setRootCause(RootCauseConfiguration rootCause) {
    this.rootCause = rootCause;
  }
}
