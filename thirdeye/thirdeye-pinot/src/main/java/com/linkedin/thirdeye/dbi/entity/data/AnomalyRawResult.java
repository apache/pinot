package com.linkedin.thirdeye.dbi.entity.data;

import com.linkedin.thirdeye.dbi.entity.base.AbstractJsonEntity;

public class AnomalyRawResult extends AbstractJsonEntity {
  long anomalyFunctionId;
  long anomalyFeedbackId;
  long jobId;
  long startTime;
  long endTime;
  String dimensionValue;
  boolean merged;
  boolean dataMissing;

  public long getAnomalyFeedbackId() {
    return anomalyFeedbackId;
  }

  public void setAnomalyFeedbackId(long anomalyFeedbackId) {
    this.anomalyFeedbackId = anomalyFeedbackId;
  }

  public long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public boolean isDataMissing() {
    return dataMissing;
  }

  public void setDataMissing(boolean dataMissing) {
    this.dataMissing = dataMissing;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }

  public void setDimensionValue(String dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public boolean isMerged() {
    return merged;
  }

  public void setMerged(boolean merged) {
    this.merged = merged;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }
}
