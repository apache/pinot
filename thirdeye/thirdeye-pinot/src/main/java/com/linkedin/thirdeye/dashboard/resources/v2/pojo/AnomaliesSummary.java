package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class AnomaliesSummary {
  Long metricId;
  String metricName;
  long startTime;
  long endTime;
  int numAnomalies;
  int numAnomaliesResolved;
  int numAnomaliesUnresolved;

  public int getNumAnomalies() {
    return numAnomalies;
  }
  public void setNumAnomalies(int numAnomalies) {
    this.numAnomalies = numAnomalies;
  }
  public int getNumAnomaliesResolved() {
    return numAnomaliesResolved;
  }
  public void setNumAnomaliesResolved(int numAnomaliesResolved) {
    this.numAnomaliesResolved = numAnomaliesResolved;
  }
  public int getNumAnomaliesUnresolved() {
    return numAnomaliesUnresolved;
  }
  public void setNumAnomaliesUnresolved(int numAnomaliesUnresolved) {
    this.numAnomaliesUnresolved = numAnomaliesUnresolved;
  }
  public Long getMetricId() {
    return metricId;
  }
  public void setMetricId(Long metricId) {
    this.metricId = metricId;
  }
  public String getMetricName() {
    return metricName;
  }
  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
  public long getStartTime() {
    return startTime;
  }
  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
  public long getEndTime() {
    return endTime;
  }
  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }


}
