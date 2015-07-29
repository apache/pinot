package com.linkedin.thirdeye.anomaly.api;

import org.apache.commons.math3.util.Pair;

import com.linkedin.thirdeye.anomaly.api.external.AnomalyResult;

/**
 * This class models a histogram of the joint distribution of anomaly scores and volumes.
 */
public class AnomalyResultDistribution {

  private final int anomalyScoreNumBuckets;
  private final Pair<Double, Double> anomalyScoreRange;

  private final int anomalyVolumeNumBuckets;
  private final Pair<Double, Double> anomalyVolumeRange;

  private final int[][] counts;

  public AnomalyResultDistribution(int anomalyScoreNumBuckets, Pair<Double, Double> anomalyScoreRange,
      int anomalyVolumeNumBuckets, Pair<Double, Double> anomalyVolumeRange) {
    super();
    this.anomalyScoreNumBuckets = anomalyScoreNumBuckets;
    this.anomalyScoreRange = anomalyScoreRange;
    this.anomalyVolumeNumBuckets = anomalyVolumeNumBuckets;
    this.anomalyVolumeRange = anomalyVolumeRange;

    counts = new int[anomalyScoreNumBuckets][anomalyVolumeNumBuckets];
  }

  public void addValue(double anomalyScore, double anomalyVolume) {
    int anomalyScoreIndex = getAnomalyScoreBucketIndex(anomalyScore);
    int anomalyBucketIndex = getAnomalyVolumeBucketIndex(anomalyVolume);
    counts[anomalyScoreIndex][anomalyBucketIndex]++;
  }

  public void addValue(AnomalyResult anomalyResult) {
    addValue(anomalyResult.getAnomalyScore(), anomalyResult.getAnomalyVolume());
  }

  public int[] getAnomalyVolumeHistogram() {
    int[] result = new int[anomalyVolumeNumBuckets];
    // get column sum
    for (int i = 0; i < anomalyScoreNumBuckets; i++) {
      for (int j = 0; j < anomalyVolumeNumBuckets; j++) {
        result[j] += counts[i][j];
      }
    }
    return result;
  }

  public int[] getAnomalyScoreHistogram() {
    int[] result = new int[anomalyScoreNumBuckets];
    // get row sum
    for (int i = 0; i < anomalyScoreNumBuckets; i++) {
      for (int j = 0; j < anomalyVolumeNumBuckets; j++) {
        result[i] += counts[i][j];
      }
    }
    return result;
  }

  public int getNumBucketsAnomalyScore() {
    return anomalyScoreNumBuckets;
  }

  public int getNumBucketsAnomalyVolume() {
    return anomalyVolumeNumBuckets;
  }

  public Pair<Double, Double> getAnomalyScoreRange() {
    return anomalyScoreRange;
  }

  public Pair<Double, Double> getAnomalyVolumeRange() {
    return anomalyVolumeRange;
  }

  public int getAnomalyScoreNumBuckets() {
    return anomalyScoreNumBuckets;
  }

  public int getAnomalyVolumeNumBuckets() {
    return anomalyVolumeNumBuckets;
  }

  public int[][] getCounts() {
    return counts;
  }

  private int getAnomalyVolumeBucketIndex(double anomalyVolume) {
    double anomalyVolumeBucketSize =
        (anomalyVolumeRange.getSecond() - anomalyVolumeRange.getFirst()) / anomalyVolumeNumBuckets;
    int anomalyVolumeIndex = (int) ((anomalyVolume - anomalyVolumeRange.getFirst()) / anomalyVolumeBucketSize);
    if (anomalyVolumeIndex < 0) {
      anomalyVolumeIndex = 0;
    } else if (anomalyVolumeIndex >= anomalyVolumeNumBuckets) {
      anomalyVolumeIndex = anomalyVolumeNumBuckets - 1;
    }
    return anomalyVolumeIndex;
  }

  private int getAnomalyScoreBucketIndex(double anomalyScore) {
    double anomalyScoreBucketSize =
        (anomalyScoreRange.getSecond() - anomalyScoreRange.getFirst()) / anomalyScoreNumBuckets;
    int anomalyScoreIndex = (int) ((anomalyScore - anomalyScoreRange.getFirst()) / anomalyScoreBucketSize);
    if (anomalyScoreIndex < 0) {
      anomalyScoreIndex = 0;
    } else if (anomalyScoreIndex >= anomalyScoreNumBuckets) {
      anomalyScoreIndex = anomalyScoreNumBuckets - 1;
    }
    return anomalyScoreIndex;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < anomalyScoreNumBuckets; i++) {
      for (int j = 0; j < anomalyVolumeNumBuckets; j++) {
        sb.append("[").append(counts[i][j]).append("] ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }


}
