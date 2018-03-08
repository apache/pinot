package com.linkedin.thirdeye.anomaly.monitor;

import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;

public class MonitorTaskInfo implements TaskInfo {

  private MonitorType monitorType;
  private int defaultRetentionDays;
  private int completedJobRetentionDays;
  private int detectionStatusRetentionDays;
  private int rawAnomalyRetentionDays;

  public MonitorTaskInfo() {

  }

  public MonitorType getMonitorType() {
    return monitorType;
  }

  public void setMonitorType(MonitorType monitorType) {
    this.monitorType = monitorType;
  }

  public int getCompletedJobRetentionDays() {
    return completedJobRetentionDays;
  }

  public void setCompletedJobRetentionDays(int jobTaskRetentionDays) {
    this.completedJobRetentionDays = jobTaskRetentionDays;
  }

  public int getDefaultRetentionDays() {
    return defaultRetentionDays;
  }

  public void setDefaultRetentionDays(int defaultRetentionDays) {
    this.defaultRetentionDays = defaultRetentionDays;
  }

  public int getDetectionStatusRetentionDays() {
    return detectionStatusRetentionDays;
  }

  public void setDetectionStatusRetentionDays(int detectionStatusRetentionDays) {
    this.detectionStatusRetentionDays = detectionStatusRetentionDays;
  }

  public int getRawAnomalyRetentionDays() {
    return rawAnomalyRetentionDays;
  }

  public void setRawAnomalyRetentionDays(int rawAnomalyRetentionDays) {
    this.rawAnomalyRetentionDays = rawAnomalyRetentionDays;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MonitorTaskInfo that = (MonitorTaskInfo) o;
    return completedJobRetentionDays == that.completedJobRetentionDays
        && defaultRetentionDays == that.defaultRetentionDays
        && detectionStatusRetentionDays == that.detectionStatusRetentionDays
        && rawAnomalyRetentionDays == that.rawAnomalyRetentionDays && monitorType == that.monitorType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(monitorType, completedJobRetentionDays, defaultRetentionDays, detectionStatusRetentionDays,
        rawAnomalyRetentionDays);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("monitorType", monitorType)
        .add("completedJobRetentionDays", completedJobRetentionDays)
        .add("defaultRetentionDays", defaultRetentionDays)
        .add("detectionStatusRetentionDays", detectionStatusRetentionDays)
        .add("rawAnomalyRetentionDays", rawAnomalyRetentionDays)
        .toString();
  }
}
