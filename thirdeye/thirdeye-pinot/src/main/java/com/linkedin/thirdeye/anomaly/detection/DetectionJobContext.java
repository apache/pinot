package com.linkedin.thirdeye.anomaly.detection;

import java.util.List;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public class DetectionJobContext extends JobContext {

  private Long anomalyFunctionId;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private List<Long> startTimes;
  private List<Long> endTimes;


  public List<Long> getStartTimes() {
    return startTimes;
  }

  public void setStartTimes(List<Long> startTimes) {
    this.startTimes = startTimes;
  }

  public List<Long> getEndTimes() {
    return endTimes;
  }

  public void setEndTimes(List<Long> endTimes) {
    this.endTimes = endTimes;
  }

  public Long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(Long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public AnomalyFunctionDTO getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionDTO anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

}
