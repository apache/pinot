package com.linkedin.thirdeye.datalayer.entity;

public class TaskIndex extends AbstractIndexEntity {
  String name;
  String status;
  String type;
  long startTime;
  long endTime;
  long jobId;
  long workerId;
  int version;

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(long workerId) {
    this.workerId = workerId;
  }

  @Override
  public String toString() {
    return "TaskIndex [name=" + name + ", status=" + status + ", type=" + type + ", startTime="
        + startTime + ", endTime=" + endTime + ", jobId=" + jobId + ", workerId=" + workerId
        + ", version=" + version + ", baseId=" + baseId + ", id=" + id + ", createTime="
        + createTime + ", updateTime=" + updateTime + "]";
  }
  
}
