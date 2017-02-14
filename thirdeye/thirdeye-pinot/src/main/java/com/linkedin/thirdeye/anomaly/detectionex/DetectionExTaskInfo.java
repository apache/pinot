package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;


public class DetectionExTaskInfo implements TaskInfo {

  private long jobExecutionId;
  private AnomalyFunctionExDTO anomalyFunctionSpec;

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public AnomalyFunctionExDTO getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionExDTO anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

}
