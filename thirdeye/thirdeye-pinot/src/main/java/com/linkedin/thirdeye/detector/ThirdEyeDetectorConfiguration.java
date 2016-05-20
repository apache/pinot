package com.linkedin.thirdeye.detector;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeDetectorConfiguration extends ThirdEyeConfiguration {
  private String dashboardHost;

  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }

  public String getDashboardHost() {
    return dashboardHost;
  }

  public void setDashboardHost(String dashboardHost) {
    this.dashboardHost = dashboardHost;
  }

}
