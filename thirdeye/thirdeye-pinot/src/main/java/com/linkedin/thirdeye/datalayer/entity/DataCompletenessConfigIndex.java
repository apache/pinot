package com.linkedin.thirdeye.datalayer.entity;

public class DataCompletenessConfigIndex extends AbstractIndexEntity {

  String dataset;
  long dateToCheckInMS;
  String dateToCheckInSDF;
  boolean dataComplete;
  double percentComplete;

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public long getDateToCheckInMS() {
    return dateToCheckInMS;
  }

  public void setDateToCheckInMS(long dateToCheckInMS) {
    this.dateToCheckInMS = dateToCheckInMS;
  }

  public String getDateToCheckInSDF() {
    return dateToCheckInSDF;
  }

  public void setDateToCheckInSDF(String dateToCheckInSDF) {
    this.dateToCheckInSDF = dateToCheckInSDF;
  }

  public boolean isDataComplete() {
    return dataComplete;
  }

  public void setDataComplete(boolean dataComplete) {
    this.dataComplete = dataComplete;
  }

  public double getPercentComplete() {
    return percentComplete;
  }

  public void setPercentComplete(double percentComplete) {
    this.percentComplete = percentComplete;
  }
}
