package com.linkedin.thirdeye.anomaly.detection;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

public class DetectionJobContext extends JobContext {

  private Long anomalyFunctionId;
  private DateTime windowStartTime;
  private DateTime windowEndTime;
  private AnomalyFunctionSpec anomalyFunctionSpec;

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public DateTime getWindowStartTime() {
    return windowStartTime;
  }

  public void setWindowStartTime(DateTime windowStartTime) {
    this.windowStartTime = windowStartTime;
  }

  public DateTime getWindowEndTime() {
    return windowEndTime;
  }

  public void setWindowEndTime(DateTime windowEndTime) {
    this.windowEndTime = windowEndTime;
  }

  public AnomalyFunctionSpec getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionSpec anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

}
