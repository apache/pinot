package com.linkedin.thirdeye.datalayer.entity;

public class DetectionStatusIndex extends AbstractIndexEntity {

  long functionId;
  String dataset;
  long dateToCheckInMS;
  String dateToCheckInSDF;
  boolean detectionRun;


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


  public long getFunctionId() {
    return functionId;
  }


  public void setFunctionId(long functionId) {
    this.functionId = functionId;
  }


  public boolean isDetectionRun() {
    return detectionRun;
  }


  public void setDetectionRun(boolean detectionRun) {
    this.detectionRun = detectionRun;
  }
}
