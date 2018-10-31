package com.linkedin.thirdeye.anomaly.classification;


public class ClassificationJobConfig {
  private long maxMonitoringWindowInMS;
  private boolean forceSyncDetectionJobs;

  public ClassificationJobConfig() {
    maxMonitoringWindowInMS = 259200000L; // 3 days
    forceSyncDetectionJobs = false;
  }

  public long getMaxMonitoringWindowSizeInMS() {
    return maxMonitoringWindowInMS;
  }

  public void setMaxMonitoringWindowInMS(long maxMonitoringWindowInMS) {
    this.maxMonitoringWindowInMS = maxMonitoringWindowInMS;
  }

  public boolean getForceSyncDetectionJobs() {
    return forceSyncDetectionJobs;
  }

  public void setForceSyncDetectionJobs(boolean forceSyncDetectionJobs) {
    this.forceSyncDetectionJobs = forceSyncDetectionJobs;
  }
}
