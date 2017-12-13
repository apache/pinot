package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import java.util.Map;

public abstract class BaseDetectionOnboardJob implements DetectionOnboardJob {
  private final String jobName;
  protected Map<String, String> properties;

  public BaseDetectionOnboardJob(String jobName) {
    this.jobName = jobName;
  }

  public String getName() {
    return jobName;
  }

  public void initialize(Map<String, String> properties) {
    Preconditions.checkNotNull(properties);
    this.properties = properties;
  }
}
