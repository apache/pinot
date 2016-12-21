package com.linkedin.thirdeye.dashboard.resources.v2.pojo;

public class AnomaliesSummary {
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


}
