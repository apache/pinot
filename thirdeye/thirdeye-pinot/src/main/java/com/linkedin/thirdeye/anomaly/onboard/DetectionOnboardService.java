package com.linkedin.thirdeye.anomaly.onboard;

import java.util.Map;

public interface DetectionOnboardService {
  /**
   * Creates and submit the job to executor service.
   *
   * @param job the job to be executed.
   * @param properties the properties to initialize the job.
   *
   * @return The job ID.
   */
  long createDetectionOnboardingJob(DetectionOnboardJob job, Map<String, String> properties);

  /**
   * Returns the execution status of the job.
   *
   * @param jobId the id of the job.
   *
   * @return the execution status of the specified job.
   */
  DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId);
}
