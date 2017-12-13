package com.linkedin.thirdeye.anomaly.onboard.tasks;

import com.linkedin.thirdeye.anomaly.onboard.BaseDetectionOnboardJob;
import com.linkedin.thirdeye.anomaly.onboard.DetectionOnboardTask;
import java.util.List;
import org.apache.commons.configuration.Configuration;

public class DefaultDetectionOnboardJob extends BaseDetectionOnboardJob {

  public DefaultDetectionOnboardJob(String jobName) {
    super(jobName);
  }

  // TODO: Implement this method
  @Override
  public Configuration getTaskConfiguration() {
    throw new UnsupportedOperationException();
  }

  // TODO: Implement this method
  @Override
  public List<DetectionOnboardTask> getTasks() {
    throw new UnsupportedOperationException();
  }
}
