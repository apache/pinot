package com.linkedin.thirdeye.anomaly.onboard;

import java.util.Map;

public interface DetectionOnboardService {
  DetectionOnboardJobStatus createDetectionOnboardingJob(DetectionOnboardJob job, Map<String, String> properties);

  DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId);
}
