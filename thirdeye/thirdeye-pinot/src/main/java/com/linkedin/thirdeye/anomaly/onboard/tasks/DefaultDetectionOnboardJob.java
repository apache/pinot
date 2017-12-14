package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTask;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;

public class DefaultDetectionOnboardJob extends BaseDetectionOnboardJob {

  public DefaultDetectionOnboardJob(String jobName, Map<String, String> properties) {
    super(jobName, properties);
  }

  // TODO: Implement this method
  @Override
  public Configuration getTaskConfiguration() {
    return new MapConfiguration(Collections.emptyMap());
  }

  // TODO: Implement this method
  @Override
  public List<DetectionOnboardTask> getTasks() {
    return Collections.emptyList();
  }
}
