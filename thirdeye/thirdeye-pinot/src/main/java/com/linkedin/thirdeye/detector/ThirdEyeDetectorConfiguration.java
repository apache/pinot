package com.linkedin.thirdeye.detector;

import com.linkedin.thirdeye.common.ThirdEyeConfiguration;

public class ThirdEyeDetectorConfiguration extends ThirdEyeConfiguration {
  public String getFunctionConfigPath() {
    return getRootDir() + "/detector-config/anomaly-functions/functions.properties";
  }
}
