package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.configs.AuthConfiguration;
import com.linkedin.thirdeye.dashboard.configs.ResourceConfiguration;
import java.util.List;


public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {
  AuthConfiguration authConfig;
  RootCauseConfiguration rootCause;
  List<ResourceConfiguration> resourceConfig;
  String onboardingHost;

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

  public AuthConfiguration getAuthConfig() {
    return authConfig;
  }

  public void setAuthConfig(AuthConfiguration authConfig) {
    this.authConfig = authConfig;
  }

  public String getOnboardingHost() {
    return onboardingHost;
  }

  public void setOnboardingHost(String onboardingHost) {
    this.onboardingHost = onboardingHost;
  }
}
