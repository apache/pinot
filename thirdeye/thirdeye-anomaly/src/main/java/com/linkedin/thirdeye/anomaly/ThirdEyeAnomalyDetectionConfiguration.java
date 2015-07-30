package com.linkedin.thirdeye.anomaly;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class ThirdEyeAnomalyDetectionConfiguration {

  public enum Mode {
    RuleBased, Generic,
  }

  /** The time range (start, end) to run anomaly detection on. */
  private TimeRange explicitTimeRange;

  /** The mode determining how to interpret the anomaly database function specifications. */
  private Mode mode;

  /** The interval at which anomaly detection tasks are scheduled to run. */
  private TimeGranularity detectionInterval;

  /** An additional delay to running detection to account for data arrival at the third eye server */
  private TimeGranularity detectionLag;

  /** The configuration for the anomaly database including: url, function table, anomaly table, user credentials */
  private AnomalyDatabaseConfig anomalyDatabaseConfig;

  /** List of anomaly detection driver configurations pertaining to each collection */
  private List<AnomalyDetectionDriverConfig> collectionDriverConfigurations;

  @JsonProperty
  public TimeRange getExplicitTimeRange() {
    return explicitTimeRange;
  }

  public void setExplicitTimeRange(TimeRange explicitTimeRange) {
    this.explicitTimeRange = explicitTimeRange;
  }

  @JsonProperty
  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  @JsonProperty
  public TimeGranularity getDetectionInterval() {
    return detectionInterval;
  }

  public void setDetectionInterval(TimeGranularity detectionInterval) {
    this.detectionInterval = detectionInterval;
  }

  @JsonProperty
  public TimeGranularity getDetectionLag() {
    return detectionLag;
  }

  public void setDetectionLag(TimeGranularity detectionLag) {
    this.detectionLag = detectionLag;
  }

  @JsonProperty
  public AnomalyDatabaseConfig getAnomalyDatabaseConfig() {
    return anomalyDatabaseConfig;
  }

  public void setAnomalyDatabaseConfig(AnomalyDatabaseConfig dbConfig) {
    this.anomalyDatabaseConfig = dbConfig;
  }

  @JsonProperty
  public List<AnomalyDetectionDriverConfig> getCollectionDriverConfigurations() {
    return collectionDriverConfigurations;
  }

  public void setCollectionDriverConfigurations(List<AnomalyDetectionDriverConfig> collections) {
    this.collectionDriverConfigurations = collections;
  }
}
