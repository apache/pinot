package com.linkedin.thirdeye.anomaly.api.external;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.anomaly.api.ResultProperties;

/**
 * Result of AnomalyDetectionFunction analyze
 */
public final class AnomalyResult implements Comparable<AnomalyResult>
{
  /** Whether this time window is anomalous */
  private final boolean isAnomaly;

  /** The timestamp of the anomaly */
  private final long timeWindow;

  /** A function specific score indicating the significance of the anomaly. */
  private final double anomalyScore;

  /** A function specific estimate of the volume of the anomaly. */
  private double anomalyVolume;

  /**
   * Additional data not fixed in anomaly table schema. The result of toString() is stored as the properties column.
   */
  private final ResultProperties properties;

  public AnomalyResult(boolean isAnomaly, long timeWindow, double anomalyScore, double anomalyVolume)
  {
    this(isAnomaly, timeWindow, anomalyScore, anomalyVolume, new ResultProperties());
  }

  public AnomalyResult(boolean isAnomaly, long timeWindow, double anomalyScore, double anomalyVolume,
      ResultProperties properties)
  {
    this.isAnomaly = isAnomaly;
    this.properties = properties;
    this.timeWindow = timeWindow;
    this.anomalyScore = anomalyScore;
    this.anomalyVolume = anomalyVolume;
  }

  public boolean isAnomaly()
  {
    return isAnomaly;
  }

  public double getAnomalyScore() {
    return anomalyScore;
  }

  public void setAnomalyVolume(double anomalyVolume) {
    this.anomalyVolume = anomalyVolume;
  }

  public double getAnomalyVolume() {
    return anomalyVolume;
  }

  public ResultProperties getProperties()
  {
    return properties;
  }

  public long getTimeWindow() {
    return timeWindow;
  }

  @Override
  public int compareTo(AnomalyResult o) {
    return (int) TimeUnit.MILLISECONDS.toSeconds(timeWindow - o.timeWindow);
  }

}
