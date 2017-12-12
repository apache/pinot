package com.linkedin.thirdeye.anomaly.onboard;

import java.util.Map;

public interface DetectionOnboardService {
  Long createDetectionOnboardingJob(String jobName, Map<String, String> properties);

  DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId);
}
