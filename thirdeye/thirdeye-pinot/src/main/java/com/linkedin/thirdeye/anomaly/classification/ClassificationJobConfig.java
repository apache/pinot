package com.linkedin.thirdeye.anomaly.classification;


public class ClassificationJobConfig {
  private long maxLookbackLength;
  private boolean forceSyncDetectionJobs;

  public ClassificationJobConfig() {
    maxLookbackLength = 259200000L; // 3 days
    forceSyncDetectionJobs = false;
  }

  public long getMaxLookbackLength() {
    return maxLookbackLength;
  }

  public void setMaxLookbackLength(long maxLookbackLength) {
    this.maxLookbackLength = maxLookbackLength;
  }

  public boolean getForceSyncDetectionJobs() {
    return forceSyncDetectionJobs;
  }

  public void setForceSyncDetectionJobs(boolean forceSyncDetectionJobs) {
    this.forceSyncDetectionJobs = forceSyncDetectionJobs;
  }
}
