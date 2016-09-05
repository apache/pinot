package com.linkedin.thirdeye.anomaly.detection;

import java.util.Objects;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.util.CustomDateDeserializer;
import com.linkedin.thirdeye.util.CustomDateSerializer;

public class DetectionTaskInfo implements TaskInfo {

  private long jobExecutionId;

  @JsonSerialize(using = CustomDateSerializer.class)
  @JsonDeserialize(using = CustomDateDeserializer.class)
  private DateTime windowStartTime;

  @JsonSerialize(using = CustomDateSerializer.class)
  @JsonDeserialize(using = CustomDateDeserializer.class)
  private DateTime windowEndTime;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private String groupByDimension;

  public DetectionTaskInfo(long jobExecutionId, DateTime windowStartTime,
      DateTime windowEndTime, AnomalyFunctionDTO anomalyFunctionSpec, String groupByDimension) {
    this.jobExecutionId = jobExecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.groupByDimension = groupByDimension;
  }

  public DetectionTaskInfo() {

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

  public AnomalyFunctionDTO getAnomalyFunctionSpec() {
    return anomalyFunctionSpec;
  }

  public void setAnomalyFunctionSpec(AnomalyFunctionDTO anomalyFunctionSpec) {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
  }

  public String getGroupByDimension() {
    return groupByDimension;
  }

  public void setGroupByDimension(String groupByDimension) {
    this.groupByDimension = groupByDimension;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DetectionTaskInfo)) {
      return false;
    }
    DetectionTaskInfo dt = (DetectionTaskInfo) o;
    return Objects.equals(jobExecutionId, dt.getJobExecutionId())
        && Objects.equals(windowStartTime, dt.getWindowStartTime())
        && Objects.equals(windowEndTime, dt.getWindowEndTime())
        && Objects.equals(anomalyFunctionSpec, dt.getAnomalyFunctionSpec())
        && Objects.equals(groupByDimension, dt.getGroupByDimension());
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobExecutionId, windowStartTime, windowEndTime, anomalyFunctionSpec, groupByDimension);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jobExecutionId", jobExecutionId).add("windowStartTime", windowStartTime)
        .add("windowEndTime", windowEndTime).add("anomalyFunctionSpec", anomalyFunctionSpec)
        .add("groupByDimension", groupByDimension).toString();
  }
}
