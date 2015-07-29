package com.linkedin.thirdeye.anomaly.api.external;

import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.anomaly.api.ResultProperties;

/**
 * Result of AnomalyDetectionFunction analyze
 */
public final class AnomalyResult implements Comparable<AnomalyResult>
{
  private final boolean isAnomaly;
  private final long timeWindow;
  private final double anomalyScore;

  /**
   * Anomaly volume can be automatically populated by the Thirdeye-Anomaly driver or populated by the function.
   */
  private double anomalyVolume;

  /**
   * Additional data not fixed in anomaly table schema. The result of toString() is stored as the properties column.
   */
  private final ResultProperties properties;

  public AnomalyResult(boolean isAnomaly, long timeWindow, double anomalyScore)
  {
    this(isAnomaly, timeWindow, anomalyScore, new ResultProperties());
  }

  public AnomalyResult(boolean isAnomaly, long timeWindow, double anomalyScore, ResultProperties properties)
  {
    this.isAnomaly = isAnomaly;
    this.properties = properties;
    this.timeWindow = timeWindow;
    this.anomalyScore = anomalyScore;
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
