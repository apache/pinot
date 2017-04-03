package com.linkedin.thirdeye.anomaly.grouping;


public class GroupingConfiguration {
  private long maxLookbackLength;

  public GroupingConfiguration() {
    maxLookbackLength = 259200000L; // 3 days
  }

  public long getMaxLookbackLength() {
    return maxLookbackLength;
  }

  public void setMaxLookbackLength(long maxLookbackLength) {
    this.maxLookbackLength = maxLookbackLength;
  }
}
