package com.linkedin.thirdeye.anomaly.classification;


public class ClassificationJobConfig {
  private long maxLookbackLength;

  public ClassificationJobConfig() {
    maxLookbackLength = 259200000L; // 3 days
  }

  public long getMaxLookbackLength() {
    return maxLookbackLength;
  }

  public void setMaxLookbackLength(long maxLookbackLength) {
    this.maxLookbackLength = maxLookbackLength;
  }
}
