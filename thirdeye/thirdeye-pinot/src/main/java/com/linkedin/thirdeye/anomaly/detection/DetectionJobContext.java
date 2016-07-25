package com.linkedin.thirdeye.anomaly.detection;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;

public class DetectionJobContext extends JobContext {

  private Long anomalyFunctionId;
  private String windowStartIso;
  private String windowEndIso;
  private DateTime windowStart;
  private DateTime windowEnd;
  private AnomalyFunctionSpec anomalyFunctionSpec;

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public String getWindowStartIso() {
    return windowStartIso;
  }

  public void setWindowStartIso(String windowStartIso) {
    this.windowStartIso = windowStartIso;
  }

  public String getWindowEndIso() {
    return windowEndIso;
  }

  public void setWindowEndIso(String windowEndIso) {
    this.windowEndIso = windowEndIso;
  }

  public DateTime getWindowStart() {
    return windowStart;
  }

  public void setWindowStart(DateTime windowStart) {
    this.windowStart = windowStart;
  }

  public DateTime getWindowEnd() {
    return windowEnd;
  }

  public void setWindowEnd(DateTime windowEnd) {
    this.windowEnd = windowEnd;
  }

  public AnomalyFunctionSpec getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionSpec anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

}
