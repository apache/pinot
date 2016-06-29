package com.linkedin.thirdeye.anomaly;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;

public class TaskInfo {

  private String jobName;
  private long jobExecutionId;

  private DateTime windowStartTime;
  private DateTime windowEndTime;
  private AnomalyFunctionSpec anomalyFunctionSpec;
  private String groupByDimension;

  public TaskInfo(String jobName, long jobExecutionId, DateTime windowStartTime, DateTime windowEndTime,
      AnomalyFunctionSpec anomalyFunctionSpec, String groupByDimension) {
    this.jobName = jobName;
    this.jobExecutionId = jobExecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.groupByDimension = groupByDimension;
  }

  public TaskInfo() {

  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
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

  public String getGroupByDimension() {
    return groupByDimension;
  }

  public void setGroupByDimension(String groupByDimension) {
    this.groupByDimension = groupByDimension;
  }

}
