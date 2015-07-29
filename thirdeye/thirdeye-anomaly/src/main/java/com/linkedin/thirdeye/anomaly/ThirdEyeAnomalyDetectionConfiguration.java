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

  private TimeRange explicitTimeRange;

  private Mode mode;

  private TimeGranularity detectionInterval;
  private TimeGranularity detectionLag;

  private AnomalyDatabaseConfig anomalyDatabase;

  private List<AnomalyDetectionDriverConfig> collections;

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
  public AnomalyDatabaseConfig getAnomalyDatabase() {
    return anomalyDatabase;
  }

  public void setAnomalyDatabase(AnomalyDatabaseConfig dbConfig) {
    this.anomalyDatabase = dbConfig;
  }

  @JsonProperty
  public List<AnomalyDetectionDriverConfig> getCollections() {
    return collections;
  }

  public void setCollections(List<AnomalyDetectionDriverConfig> collections) {
    this.collections = collections;
  }
}
