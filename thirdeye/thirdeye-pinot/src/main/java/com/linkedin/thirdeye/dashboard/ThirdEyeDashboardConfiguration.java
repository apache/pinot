package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;


public class ThirdEyeDashboardConfiguration extends ThirdEyeConfiguration {

  RootCauseConfiguration rootCause;

  public RootCauseConfiguration getRootCause() {
    return rootCause;
  }

  public void setRootCause(RootCauseConfiguration rootCause) {
    this.rootCause = rootCause;
  }
}
