package com.linkedin.thirdeye.anomaly.reporting;

import org.apache.commons.math3.util.Pair;

/**
 * This class models a histogram of the joint distribution of anomaly scores and another real-valued field.
 */
public class AnomalyResultDistribution {

  private final int anomalyScoreNumBuckets;
  private final Pair<Double, Double> anomalyScoreRange;

  private final int anomalyOtherNumBuckets;
  private final Pair<Double, Double> anomalyOtherRange;

  private final int[][] counts;

  /**
   * Create 2d distribution of anomaly scores
   *
   * @param anomalyScoreNumBuckets
   *  Number of buckets for AnomalyScore
   * @param anomalyScoreRange
   *  AnomalyScore range [start, end]
   * @param anomalyOtherNumBuckets
   *  Number of buckets for AnomalyVolume
   * @param anomalyOtherRange (e.g.,
   *  AnomalyOther range [start, end]
   */
  public AnomalyResultDistribution(int anomalyScoreNumBuckets, Pair<Double, Double> anomalyScoreRange,
      int anomalyOtherNumBuckets, Pair<Double, Double> anomalyOtherRange) {
    super();
    this.anomalyScoreNumBuckets = anomalyScoreNumBuckets;
    this.anomalyScoreRange = anomalyScoreRange;
    this.anomalyOtherNumBuckets = anomalyOtherNumBuckets;
    this.anomalyOtherRange = anomalyOtherRange;

    counts = new int[anomalyScoreNumBuckets][anomalyOtherNumBuckets];
  }

  /**
   * Add a value to the joint distribution
   *
   * @param anomalyScore
   * @param anomalyOther
   */
  public void addValue(double anomalyScore, double anomalyOther) {
    int anomalyScoreIndex = getAnomalyScoreBucketIndex(anomalyScore);
    int anomalyOtherIndex = getAnomalyOtherBucketIndex(anomalyOther);
    counts[anomalyScoreIndex][anomalyOtherIndex]++;
  }

  /**
   * @return
   *  An array of counts across the other axis
   */
  public int[] getAnomalyOtherHistogram() {
    int[] result = new int[anomalyOtherNumBuckets];
    // get column sum
    for (int i = 0; i < anomalyScoreNumBuckets; i++) {
      for (int j = 0; j < anomalyOtherNumBuckets; j++) {
        result[j] += counts[i][j];
      }
    }
    return result;
  }

  /**
   * @return
   *  An array of counts across the score axis
   */
  public int[] getAnomalyScoreHistogram() {
    int[] result = new int[anomalyScoreNumBuckets];
    // get row sum
    for (int i = 0; i < anomalyScoreNumBuckets; i++) {
      for (int j = 0; j < anomalyOtherNumBuckets; j++) {
        result[i] += counts[i][j];
      }
    }
    return result;
  }

  /**
   * @return
   *  The range of the score axis
   */
  public Pair<Double, Double> getAnomalyScoreRange() {
    return anomalyScoreRange;
  }

  /**
   * @return
   *  The range of the other axis
   */
  public Pair<Double, Double> getAnomalyOtherRange() {
    return anomalyOtherRange;
  }

  /**
   * @return
   *  Number of buckets on the score axis
   */
  public int getAnomalyScoreNumBuckets() {
    return anomalyScoreNumBuckets;
  }

  /**
   * @return
   *  Number of buckets on the other axis
   */
  public int getAnomalyOtherNumBuckets() {
    return anomalyOtherNumBuckets;
  }

  /**
   * @return
   *  The internal histogram of counts.
   */
  public int[][] getCounts() {
    return counts;
  }

  private int getAnomalyOtherBucketIndex(double anomalyVolume) {
    double anomalyVolumeBucketSize =
        (anomalyOtherRange.getSecond() - anomalyOtherRange.getFirst()) / anomalyOtherNumBuckets;
    int anomalyVolumeIndex = (int) ((anomalyVolume - anomalyOtherRange.getFirst()) / anomalyVolumeBucketSize);
    if (anomalyVolumeIndex < 0) {
      anomalyVolumeIndex = 0;
    } else if (anomalyVolumeIndex >= anomalyOtherNumBuckets) {
      anomalyVolumeIndex = anomalyOtherNumBuckets - 1;
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
      for (int j = 0; j < anomalyOtherNumBuckets; j++) {
        sb.append("[").append(counts[i][j]).append("] ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }


}
