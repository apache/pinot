package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;


/**
 * The Detection alert task info.
 */
public class DetectionAlertTaskInfo implements TaskInfo {

  private long detectionAlertConfigId;

  public DetectionAlertTaskInfo() {
  }

  public DetectionAlertTaskInfo(long detectionAlertConfigId) {
    this.detectionAlertConfigId = detectionAlertConfigId;
  }

  public long getDetectionAlertConfigId() {
    return detectionAlertConfigId;
  }

  public void setDetectionAlertConfigId(long detectionAlertConfigId) {
    this.detectionAlertConfigId = detectionAlertConfigId;
  }
}
