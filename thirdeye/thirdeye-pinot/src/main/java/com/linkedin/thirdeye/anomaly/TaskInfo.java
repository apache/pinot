package com.linkedin.thirdeye.anomaly;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.util.CustomDateSerializer;

public class TaskInfo {

  private long jobExecutionId;

  @JsonSerialize(using = CustomDateSerializer.class)
  private DateTime windowStartTime;

  @JsonSerialize(using = CustomDateSerializer.class)
  private DateTime windowEndTime;
  private AnomalyFunctionSpec anomalyFunctionSpec;
  private String groupByDimension;

  private AnomalyFunction anomalyFunction;


  public TaskInfo(long jobExecutionId, DateTime windowStartTime,
      DateTime windowEndTime, AnomalyFunctionSpec anomalyFunctionSpec, String groupByDimension,
      AnomalyFunction anomalyFunction) {
    this.jobExecutionId = jobExecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.groupByDimension = groupByDimension;
    this.anomalyFunction = anomalyFunction;
  }

  public TaskInfo() {

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

  public AnomalyFunction getAnomalyFunction() {
    return anomalyFunction;
  }

  public void setAnomalyFunction(AnomalyFunction anomalyFunction) {
    this.anomalyFunction = anomalyFunction;
  }



}
