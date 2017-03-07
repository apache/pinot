package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  String informedApiUrl;

  public String getInformedApiUrl() {
    return informedApiUrl;
  }

  public void setInformedApiUrl(String informedApiUrl) {
    this.informedApiUrl = informedApiUrl;
  }
}
