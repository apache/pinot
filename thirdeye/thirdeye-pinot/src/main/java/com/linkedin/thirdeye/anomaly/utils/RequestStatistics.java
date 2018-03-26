package com.linkedin.thirdeye.anomaly.utils;

import java.util.Map;


/**
 * Statistics container for data source request performance log
 */
public final class RequestStatistics {
  Map<String, Long> requestsPerDatasource;
  Map<String, Long> requestsPerDataset;
  Map<String, Long> requestsPerMetric;
  long requestsTotal;

  Map<String, Long> successPerDatasource;
  Map<String, Long> successPerDataset;
  Map<String, Long> successPerMetric;
  long successTotal;

  Map<String, Long> failurePerDatasource;
  Map<String, Long> failurePerDataset;
  Map<String, Long> failurePerMetric;
  long failureTotal;

  Map<String, Long> durationPerDatasource;
  Map<String, Long> durationPerDataset;
  Map<String, Long> durationPerMetric;
  long durationTotal;

  public Map<String, Long> getRequestsPerDatasource() {
    return requestsPerDatasource;
  }

  public void setRequestsPerDatasource(Map<String, Long> requestsPerDatasource) {
    this.requestsPerDatasource = requestsPerDatasource;
  }

  public Map<String, Long> getRequestsPerDataset() {
    return requestsPerDataset;
  }

  public void setRequestsPerDataset(Map<String, Long> requestsPerDataset) {
    this.requestsPerDataset = requestsPerDataset;
  }

  public Map<String, Long> getRequestsPerMetric() {
    return requestsPerMetric;
  }

  public void setRequestsPerMetric(Map<String, Long> requestsPerMetric) {
    this.requestsPerMetric = requestsPerMetric;
  }

  public long getRequestsTotal() {
    return requestsTotal;
  }

  public void setRequestsTotal(long requestsTotal) {
    this.requestsTotal = requestsTotal;
  }

  public Map<String, Long> getSuccessPerDatasource() {
    return successPerDatasource;
  }

  public void setSuccessPerDatasource(Map<String, Long> successPerDatasource) {
    this.successPerDatasource = successPerDatasource;
  }

  public Map<String, Long> getSuccessPerDataset() {
    return successPerDataset;
  }

  public void setSuccessPerDataset(Map<String, Long> successPerDataset) {
    this.successPerDataset = successPerDataset;
  }

  public Map<String, Long> getSuccessPerMetric() {
    return successPerMetric;
  }

  public void setSuccessPerMetric(Map<String, Long> successPerMetric) {
    this.successPerMetric = successPerMetric;
  }

  public long getSuccessTotal() {
    return successTotal;
  }

  public void setSuccessTotal(long successTotal) {
    this.successTotal = successTotal;
  }

  public Map<String, Long> getFailurePerDatasource() {
    return failurePerDatasource;
  }

  public void setFailurePerDatasource(Map<String, Long> failurePerDatasource) {
    this.failurePerDatasource = failurePerDatasource;
  }

  public Map<String, Long> getFailurePerDataset() {
    return failurePerDataset;
  }

  public void setFailurePerDataset(Map<String, Long> failurePerDataset) {
    this.failurePerDataset = failurePerDataset;
  }

  public Map<String, Long> getFailurePerMetric() {
    return failurePerMetric;
  }

  public void setFailurePerMetric(Map<String, Long> failurePerMetric) {
    this.failurePerMetric = failurePerMetric;
  }

  public long getFailureTotal() {
    return failureTotal;
  }

  public void setFailureTotal(long failureTotal) {
    this.failureTotal = failureTotal;
  }

  public Map<String, Long> getDurationPerDatasource() {
    return durationPerDatasource;
  }

  public void setDurationPerDatasource(Map<String, Long> durationPerDatasource) {
    this.durationPerDatasource = durationPerDatasource;
  }

  public Map<String, Long> getDurationPerDataset() {
    return durationPerDataset;
  }

  public void setDurationPerDataset(Map<String, Long> durationPerDataset) {
    this.durationPerDataset = durationPerDataset;
  }

  public Map<String, Long> getDurationPerMetric() {
    return durationPerMetric;
  }

  public void setDurationPerMetric(Map<String, Long> durationPerMetric) {
    this.durationPerMetric = durationPerMetric;
  }

  public long getDurationTotal() {
    return durationTotal;
  }

  public void setDurationTotal(long durationTotal) {
    this.durationTotal = durationTotal;
  }
}
