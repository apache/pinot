package com.linkedin.thirdeye.anomaly;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;

/**
 *
 */
public class ThirdEyeAnomalyDetectionConfiguration {

  /** Built in modes of operation that determine how function definitions in the anomaly database are interpreted */
  public enum Mode {
    RULEBASED, GENERIC,
  }

  /** The mode determining how to interpret the anomaly database function specifications. */
  private Mode mode;

  /** The name of the collection */
  private String collectionName;

  /** The server hosting data */
  private String thirdEyeServerHost;

  /** The port to request data from the server */
  private short thirdEyeServerPort;

  /** The configuration for the anomaly database including: url, function table, anomaly table, user credentials */
  private AnomalyDatabaseConfig anomalyDatabaseConfig;

  /** List of anomaly detection driver configurations pertaining to each collection */
  private AnomalyDetectionDriverConfig driverConfig;

  /** Maximum amount of time to wait for tasks in a batch to finish */
  private TimeGranularity maxWaitToCompletion = new TimeGranularity(1, TimeUnit.HOURS);

  /**
   * Only run this functionId. This should only be used for testing purposes. The is_active column should be used
   * to control which functions are run in production.
   */
  private Integer functionIdToEvaluate = null;

  /*
   * The following settings are only used when the anomaly detection service is running as a standalone process.
   */

  /**
   * The time range (start, end) to run anomaly detection on.
   * This option only makes sense in a standalone mode!
   */
  private TimeRange explicitTimeRange;

  /**
   * The interval at which anomaly detection tasks are scheduled to run.
   * This option only makes sense in a standalone mode and when not running with an explicit time range!
   */
  private TimeGranularity detectionInterval;

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
  public short getThirdEyeServerPort() {
    return thirdEyeServerPort;
  }

  public void setThirdEyeServerPort(short thirdEyeServerPort) {
    this.thirdEyeServerPort = thirdEyeServerPort;
  }

  @JsonProperty
  public String getThirdEyeServerHost() {
    return thirdEyeServerHost;
  }

  public void setThirdEyeServerHost(String thirdEyeServerHost) {
    this.thirdEyeServerHost = thirdEyeServerHost;
  }

  @JsonProperty
  public TimeGranularity getDetectionInterval() {
    return detectionInterval;
  }

  public void setDetectionInterval(TimeGranularity detectionInterval) {
    this.detectionInterval = detectionInterval;
  }

  @JsonProperty
  public AnomalyDatabaseConfig getAnomalyDatabaseConfig() {
    return anomalyDatabaseConfig;
  }

  public void setAnomalyDatabaseConfig(AnomalyDatabaseConfig dbConfig) {
    this.anomalyDatabaseConfig = dbConfig;
  }

  @JsonProperty
  public TimeGranularity getMaxWaitToCompletion() {
    return maxWaitToCompletion;
  }

  public void setMaxWaitToCompletion(TimeGranularity maxWaitToCompletion) {
    this.maxWaitToCompletion = maxWaitToCompletion;
  }

  @JsonProperty
  public AnomalyDetectionDriverConfig getDriverConfig() {
    return driverConfig;
  }

  public void setDriverConfig(AnomalyDetectionDriverConfig driverConfig) {
    this.driverConfig = driverConfig;
  }

  @JsonProperty
  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  @JsonProperty
  public Integer getFunctionIdToEvaluate() {
    return functionIdToEvaluate;
  }

  public void setFunctionIdToEvaluate(Integer functionIdToEvaluate) {
    this.functionIdToEvaluate = functionIdToEvaluate;
  }

}
