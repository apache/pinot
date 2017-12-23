package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public abstract class BaseDetectionOnboardJob implements DetectionOnboardJob {
  private final String jobName;
  protected ImmutableMap<String, String> properties;

  public BaseDetectionOnboardJob(String jobName, Map<String, String> properties) {
    Preconditions.checkNotNull(jobName);
    Preconditions.checkArgument(StringUtils.isNotBlank(jobName.trim()));
    Preconditions.checkNotNull(properties);

    this.jobName = jobName;
    this.properties = ImmutableMap.copyOf(properties);
  }

  public String getName() {
    return jobName;
  }
}
