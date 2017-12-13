package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public abstract class BaseDetectionOnboardJob implements DetectionOnboardJob {
  private final String jobName;
  protected Map<String, String> properties;

  public BaseDetectionOnboardJob(String jobName) {
    Preconditions.checkNotNull(jobName);
    Preconditions.checkArgument(StringUtils.isNotBlank(jobName.trim()));
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
