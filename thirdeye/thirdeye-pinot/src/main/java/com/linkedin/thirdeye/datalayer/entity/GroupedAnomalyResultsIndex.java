package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.api.DimensionMap;

public class GroupedAnomalyResultsIndex extends AbstractIndexEntity {
  long alertConfigId;
  DimensionMap dimensions;
  long endTime;

  public long getAlertConfigId() {
    return alertConfigId;
  }

  public void setAlertConfigId(long alertConfigId) {
    this.alertConfigId = alertConfigId;
  }

  public DimensionMap getDimensions() {
    return dimensions;
  }

  public void setDimensions(DimensionMap dimensions) {
    this.dimensions = dimensions;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }
}
