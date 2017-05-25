package com.linkedin.thirdeye.anomaly.merge;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

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
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalyMergeConfig {
  private AnomalyMergeStrategy mergeStrategy = AnomalyMergeStrategy.FUNCTION;
  private long sequentialAllowedGap = 30_000; // 30 seconds
  private long maxMergeDurationLength = 12 * 60 * 60 * 1000; // 12 hours
  private String anomalyFunctionConfigTagKey = "testConfigTag";

  public AnomalyMergeStrategy getMergeStrategy() {
    return mergeStrategy;
  }

  public void setMergeStrategy(AnomalyMergeStrategy mergeStrategy) {
    this.mergeStrategy = mergeStrategy;
  }

  public long getSequentialAllowedGap() {
    return sequentialAllowedGap;
  }

  public void setAnomalyFunctionConfigTagKey(String anomalyFunctionConfigTagKey){ this.anomalyFunctionConfigTagKey =
      anomalyFunctionConfigTagKey; }

  public String getAnomalyFunctionConfigTagKey() {return this.anomalyFunctionConfigTagKey;}


  public void setSequentialAllowedGap(long sequentialAllowedGap) {
    this.sequentialAllowedGap = sequentialAllowedGap;
  }

  public long getMaxMergeDurationLength() {
    return maxMergeDurationLength;
  }

  public void setMaxMergeDurationLength(long mergeDuration) {
    this.maxMergeDurationLength = mergeDuration;
  }
}
