package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.job.JobContext;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;


public class DetectionExJobContext extends JobContext {

  private Long anomalyFunctionId;
  private AnomalyFunctionExDTO anomalyFunctionExSpec;

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

}
