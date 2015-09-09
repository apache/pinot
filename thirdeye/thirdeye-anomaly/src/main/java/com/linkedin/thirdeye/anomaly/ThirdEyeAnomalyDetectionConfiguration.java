package com.linkedin.thirdeye.anomaly;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * This class is configuration for thirdeye-anomaly detection.
 *
 * The purpose of this configuration is to:
 *  - configure a single round of anomaly detection
 *  - be generic to online and ad-hoc runs of anomaly detection
 *
 * This config does not handle:
 *  - scheduling (detection interval)
 *  - polling (how often to poll thirdeye-server)
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

  /** Provide functions with a list of anomalies they produced in the past. */
  private boolean provideAnomalyHistory = true;

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

  @JsonProperty
  public boolean isProvideAnomalyHistory() {
    return provideAnomalyHistory;
  }

  public void setProvideAnomalyHistory(boolean provideAnomalyHistory) {
    this.provideAnomalyHistory = provideAnomalyHistory;
  }

}
