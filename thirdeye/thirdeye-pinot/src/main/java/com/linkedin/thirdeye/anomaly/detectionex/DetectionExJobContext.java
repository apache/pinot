package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;


public class DetectionExJobContext extends JobContext {

  private Long anomalyFunctionId;
  private AnomalyFunctionExDTO anomalyFunctionExSpec;
  private long monitoringWindowAlignment;
  private long monitoringWindowLookback;
  private long mergeWindow;

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public AnomalyFunctionExDTO getAnomalyFunctionExSpec() {
    return anomalyFunctionExSpec;
  }

  public void setAnomalyFunctionExSpec(AnomalyFunctionExDTO anomalyFunctionSpec) {
    this.anomalyFunctionExSpec = anomalyFunctionSpec;
  }

  public long getMergeWindow() {
    return mergeWindow;
  }

  public void setMergeWindow(long mergeWindow) {
    this.mergeWindow = mergeWindow;
  }

  public long getMonitoringWindowAlignment() {
    return monitoringWindowAlignment;
  }

  public void setMonitoringWindowAlignment(long monitoringWindowAlignment) {
    this.monitoringWindowAlignment = monitoringWindowAlignment;
  }

  public long getMonitoringWindowLookback() {
    return monitoringWindowLookback;
  }

  public void setMonitoringWindowLookback(long monitoringWindowLookback) {
    this.monitoringWindowLookback = monitoringWindowLookback;
  }
}
