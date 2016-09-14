package com.linkedin.thirdeye.datalayer.pojo;

import java.sql.Timestamp;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;
import javax.persistence.Version;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly
 * job, which in turn spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the
 * workers
 */
@MappedSuperclass
public class TaskBean extends AbstractBean {

  @Enumerated(EnumType.STRING)
  @Column(name = "task_type", nullable = false)
  private TaskType taskType;

  @Column(name = "worker_id")
  private Long workerId;

  @Transient
  private Long jobId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private TaskStatus status;

  @Column(name = "task_start_time")
  private long startTime;

  @Column(name = "task_end_time")
  private long endTime;

  @Column(name = "task_info", nullable = false)
  private String taskInfo;

  @Column(name = "last_modified", insertable = false, updatable = false,
      columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
  private Timestamp lastModified;

  @Version
  @Column(name = "version", columnDefinition = "integer DEFAULT 0", nullable = false)
  private int version;

  public Long getWorkerId() {
    return workerId;
  }

  public void setWorkerId(Long workerId) {
    this.workerId = workerId;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getJobName() {
    return jobName;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public String getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(String taskInfo) {
    this.taskInfo = taskInfo;
  }

  public TaskType getTaskType() {
    return taskType;
  }

  public void setTaskType(TaskType taskType) {
    this.taskType = taskType;
  }


  public Timestamp getLastModified() {
    return lastModified;
  }

  public void setLastModified(Timestamp lastModified) {
    this.lastModified = lastModified;
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(Long jobId) {
    this.jobId = jobId;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TaskBean)) {
      return false;
    }
    TaskBean af = (TaskBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(status, af.getStatus())
        && Objects.equals(startTime, af.getStartTime()) && Objects.equals(endTime, af.getEndTime())
        && Objects.equals(taskInfo, af.getTaskInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), status, startTime, endTime, taskInfo);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("status", status)
        .add("startTime", startTime).add("endTime", endTime).add("taskInfo", taskInfo)
        .add("lastModified", lastModified).toString();
  }
}
