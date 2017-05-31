package com.linkedin.thirdeye.anomaly.merge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.List;


/**
 * Merge Configuration to hint merge module to fetch anomalies by specific group and apply certain
 * merge rule
 * <p/>
 * Check following merge parameters:
 * <p/>
 * mergeStrategy : dictates how anomalies should be grouped
 * <p/>
 * sequentialAllowedGap: allowed gap in milli seconds between sequential anomalies
 * <p/>
 * mergeDuration : length of the merged anomaly in milli seconds
 * <p/>
 * mergeablePropertyKeys : allow anomalies to be compared using function specified mergeablePropertyKeys
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalyMergeConfig {
  private AnomalyMergeStrategy mergeStrategy = AnomalyMergeStrategy.FUNCTION;
  private long sequentialAllowedGap = 30_000; // 30 seconds
  private long maxMergeDurationLength = 12 * 60 * 60 * 1000; // 12 hours
  private List<String> mergeablePropertyKeys = new ArrayList<>();

  public AnomalyMergeConfig() {
  }

  public AnomalyMergeConfig(AnomalyMergeStrategy mergeStrategy, long sequentialAllowedGap, long maxMergeDurationLength, List<String> mergeablePropertyKeys) {
    this.mergeStrategy = mergeStrategy;
    this.sequentialAllowedGap = sequentialAllowedGap;
    this.maxMergeDurationLength = maxMergeDurationLength;
    this.mergeablePropertyKeys = mergeablePropertyKeys;
  }

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

  public long getMaxMergeDurationLength() {
    return maxMergeDurationLength;
  }

  public void setMaxMergeDurationLength(long mergeDuration) {
    this.maxMergeDurationLength = mergeDuration;
  }

  public List<String> getMergeablePropertyKeys() {
    return mergeablePropertyKeys;
  }

  public void setMergeablePropertyKeys(List<String> mergeablePropertyKeys) {
    this.mergeablePropertyKeys = mergeablePropertyKeys;
  }
}