package com.linkedin.thirdeye.datalayer.entity;

import com.linkedin.thirdeye.anomaly.task.TaskConstants;

public class JobIndex extends AbstractIndexEntity {
  String name;
  String status;
  TaskConstants.TaskType type;
  long anomalyFunctionId;
  long scheduleStartTime;
  long scheduleEndTime;


  public long getScheduleStartTime() {
    return scheduleStartTime;
  }

  public void setScheduleStartTime(long scheduleStartTime) {
    this.scheduleStartTime = scheduleStartTime;
  }

  public long getScheduleEndTime() {
    return scheduleEndTime;
  }

  public void setScheduleEndTime(long scheduleEndTime) {
    this.scheduleEndTime = scheduleEndTime;
  }

  public TaskConstants.TaskType getType() {
    return type;
  }

  public void setType(TaskConstants.TaskType type) {
    this.type = type;
  }

  public long getAnomalyFunctionId() {
    return anomalyFunctionId;
  }

  public void setAnomalyFunctionId(long anomalyFunctionId) {
    this.anomalyFunctionId = anomalyFunctionId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }



  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
