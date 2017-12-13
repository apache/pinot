package com.linkedin.thirdeye.anomaly.onboard;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;

public class DefaultDetectionOnboardJob implements DetectionOnboardJob {
  private final String jobName;
  private ImmutableMap<String, String> properties = ImmutableMap.of();

  public DefaultDetectionOnboardJob(String jobName) {
    Preconditions.checkNotNull(jobName);
    this.jobName = jobName;
  }

  @Override
  public String getName() {
    return jobName;
  }

  @Override
  public Configuration getTaskConfiguration() {
    return null;
  }

  @Override
  public List<DetectionOnboardTask> getTasks() {
    return null;
  }
}
