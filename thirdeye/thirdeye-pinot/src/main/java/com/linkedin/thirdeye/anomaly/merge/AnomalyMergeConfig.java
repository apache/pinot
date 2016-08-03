package com.linkedin.thirdeye.anomaly.merge;

public class AnomalyMergeConfig {
  AnomalyMergeStrategy mergeStrategy = AnomalyMergeStrategy.FUNCTION;
  long sequentialAllowedGap = 30_000; // 30 seconds
  long mergeDuration = 12 * 60 * 60 * 1000; // 12 hours

  public AnomalyMergeStrategy getMergeStrategy() {
    return mergeStrategy;
  }

  public void setMergeStrategy(AnomalyMergeStrategy mergeStrategy) {
    this.mergeStrategy = mergeStrategy;
  }

  public long getSequentialAllowedGap() {
    return sequentialAllowedGap;
  }

  public void setSequentialAllowedGap(long sequentialAllowedGap) {
    this.sequentialAllowedGap = sequentialAllowedGap;
  }

  public long getMergeDuration() {
    return mergeDuration;
  }

  public void setMergeDuration(long mergeDuration) {
    this.mergeDuration = mergeDuration;
  }
}
