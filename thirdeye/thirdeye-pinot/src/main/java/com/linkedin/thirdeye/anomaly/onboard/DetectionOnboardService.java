package com.linkedin.thirdeye.anomaly.onboard;

import java.util.Map;

public interface DetectionOnboardService {
  long createDetectionOnboardingJob(DetectionOnboardJob job, Map<String, String> properties);

  DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId);
}
