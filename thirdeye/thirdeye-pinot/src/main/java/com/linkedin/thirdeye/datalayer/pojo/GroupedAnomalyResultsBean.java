package com.linkedin.thirdeye.datalayer.pojo;

import com.linkedin.thirdeye.api.DimensionMap;
import java.util.ArrayList;
import java.util.List;

public class GroupedAnomalyResultsBean extends AbstractBean {
  private long alertConfigId;
  private DimensionMap dimensions = new DimensionMap();
  private List<Long> anomalyResultsId = new ArrayList<>();
  // the max endTime among all its merged anomaly results
  private long endTime;
  private boolean isNotified = false;

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

  public List<Long> getAnomalyResultsId() {
    return anomalyResultsId;
  }

  public void setAnomalyResultsId(List<Long> anomalyResultsId) {
    this.anomalyResultsId = anomalyResultsId;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public boolean isNotified() {
    return isNotified;
  }

  public void setNotified(boolean notified) {
    isNotified = notified;
  }
}
