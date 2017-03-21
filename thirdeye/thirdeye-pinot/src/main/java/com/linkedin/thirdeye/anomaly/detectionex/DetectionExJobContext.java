package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;


public class DetectionExJobContext extends JobContext {

  enum DetectionJobType {
    ONLINE,
    BACKFILL
  }

  private Long anomalyFunctionId;
  private AnomalyFunctionExDTO anomalyFunctionExSpec;

  private DetectionJobType type;

  // for online
  private long monitoringWindowAlignment;
  private long monitoringWindowLookback;

  // for backfill
  private long monitoringWindowStart;
  private long monitoringWindowEnd;

  // for integrated merge
  // TODO remove when merger works
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

  public DetectionJobType getType() {
    return type;
  }

  public void setType(DetectionJobType type) {
    this.type = type;
  }
}
