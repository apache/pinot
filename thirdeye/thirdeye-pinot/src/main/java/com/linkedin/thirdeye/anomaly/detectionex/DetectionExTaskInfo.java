package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;


public class DetectionExTaskInfo implements TaskInfo {

  private long jobExecutionId;
  private AnomalyFunctionExDTO anomalyFunctionExSpec;
  private long monitoringWindowStart;
  private long monitoringWindowEnd;
  private long mergeWindow;

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public AnomalyFunctionExDTO getAnomalyFunctionExSpec() {
    return anomalyFunctionExSpec;
  }

  public void setAnomalyFunctionExSpec(AnomalyFunctionExDTO anomalyFunctionSpec) {
    this.anomalyFunctionExSpec = anomalyFunctionSpec;
  }

  public long getMonitoringWindowStart() {
    return monitoringWindowStart;
  }

  public void setMonitoringWindowStart(long monitoringWindowStart) {
    this.monitoringWindowStart = monitoringWindowStart;
  }

  public long getMonitoringWindowEnd() {
    return monitoringWindowEnd;
  }

  public void setMonitoringWindowEnd(long monitoringWindowEnd) {
    this.monitoringWindowEnd = monitoringWindowEnd;
  }

  public long getMergeWindow() {
    return mergeWindow;
  }

  public void setMergeWindow(long mergeWindow) {
    this.mergeWindow = mergeWindow;
  }
}
