package com.linkedin.thirdeye.detector;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.detector.driver.FailureEmailConfiguration;

public class ThirdEyeDetectorConfiguration extends ThirdEyeConfiguration {
  private String dashboardHost;
  private FailureEmailConfiguration failureEmailConfig;

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

  public FailureEmailConfiguration getFailureEmailConfig() {
    return failureEmailConfig;
  }

  public void setFailureEmailConfig(FailureEmailConfiguration failureEmailConfig) {
    this.failureEmailConfig = failureEmailConfig;
  }
}
