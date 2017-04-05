package com.linkedin.thirdeye.anomaly.detection;

import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.util.CustomListDateDeserializer;
import com.linkedin.thirdeye.util.CustomListDateSerializer;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobContext.DetectionJobType;

public class DetectionTaskInfo implements TaskInfo {

  private long jobExecutionId;

  @JsonSerialize(using = CustomListDateSerializer.class)
  @JsonDeserialize(using = CustomListDateDeserializer.class)
  private List<DateTime> windowStartTime;

  @JsonSerialize(using = CustomListDateSerializer.class)
  @JsonDeserialize(using = CustomListDateDeserializer.class)
  private List<DateTime> windowEndTime;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private String groupByDimension;
  private DetectionJobType detectionJobType = DetectionJobType.DEFAULT;

  public DetectionTaskInfo(long jobExecutionId, List<DateTime> windowStartTime,
      List<DateTime> windowEndTime, AnomalyFunctionDTO anomalyFunctionSpec, String groupByDimension,
      DetectionJobType detectionJobType) {
    this.jobExecutionId = jobExecutionId;
    this.windowStartTime = windowStartTime;
    this.windowEndTime = windowEndTime;
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.groupByDimension = groupByDimension;
    this.detectionJobType = detectionJobType;
  }

  public DetectionTaskInfo() {
    this.detectionJobType = DetectionJobType.DEFAULT;
  }

  public long getJobExecutionId() {
    return jobExecutionId;
  }

  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }

  public List<DateTime> getWindowStartTime() {
    return windowStartTime;
  }

  public void setWindowStartTime(List<DateTime> windowStartTime) {
    this.windowStartTime = windowStartTime;
  }

  public List<DateTime> getWindowEndTime() {
    return windowEndTime;
  }

  public void setWindowEndTime(List<DateTime> windowEndTime) {
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

  public DetectionJobType getDetectionJobType() {
    return detectionJobType;
  }

  public void setDetectionJobType(DetectionJobType detectionJobType) {
    this.detectionJobType = detectionJobType;
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
