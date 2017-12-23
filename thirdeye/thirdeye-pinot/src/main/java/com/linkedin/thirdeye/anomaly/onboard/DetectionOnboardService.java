package com.linkedin.thirdeye.anomaly.onboard;


public interface DetectionOnboardService {
  /**
   * Creates and submit the job to executor service.
   *
   * @param job the job to be executed.
   *
   * @return The job ID.
   */
  long createDetectionOnboardingJob(DetectionOnboardJob job);

  /**
   * Returns the execution status of the job.
   *
   * @param jobId the id of the job.
   *
   * @return the execution status of the specified job.
   */
  DetectionOnboardJobStatus getDetectionOnboardingJobStatus(long jobId);
}
