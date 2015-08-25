package com.linkedin.thirdeye.anomaly.api.task;

import com.linkedin.thirdeye.api.TimeRange;

/**
 * This class is used to identify the function and inputs to LocalDriverAnomalyDetectionTask
 */
public class AnomalyDetectionTaskInfo {

  private final int functionId;
  private final String functionName;
  private final String functionDescription;

  /**
   * The time for which to produce anomaly results.
   */
  private final TimeRange timeRange;

  public AnomalyDetectionTaskInfo(String functionName, int functionId, String functionDescription, TimeRange timeRange)
  {
    super();
    this.functionName = functionName;
    this.functionId = functionId;
    this.functionDescription = functionDescription;
    this.timeRange = timeRange;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public String getFunctionName() {
    return functionName;
  }

  public int getFunctionId() {
    return functionId;
  }

  public String getFunctionDescription() {
    return functionDescription;
  }

  @Override
  public String toString() {
    return "AnomalyDetectionTaskInfo [functionName=" + functionName + ", functionId=" + functionId +
        ", functionDescription=" + functionDescription + ", timeRange=" + timeRange + "]";
  }

}

