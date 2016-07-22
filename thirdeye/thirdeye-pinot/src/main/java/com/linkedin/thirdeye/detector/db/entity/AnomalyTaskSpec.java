package com.linkedin.thirdeye.detector.db.entity;

import java.sql.Timestamp;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;


/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly job, which in turn
 * spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the workers
 */
@Entity
@Table(name = "anomaly_tasks")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll", query = "SELECT at FROM AnomalyTaskSpec at"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobId", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.jobId = :jobId"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobIdAndStatusNotIn", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.jobId = :jobId AND at.status != :status"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.status = :status order by at.taskStartTime asc"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndTaskEndTime", query = "UPDATE AnomalyTaskSpec SET status = :newStatus, taskEndTime = :taskEndTime WHERE status = :oldStatus and id = :id"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatusAndWorkerId", query = "UPDATE AnomalyTaskSpec SET status = :newStatus, workerId = :workerId WHERE status = :oldStatus and id = :id"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#deleteRecordsOlderThanDaysWithStatus", query = "DELETE FROM AnomalyTaskSpec WHERE status = :status AND lastModified < :expireTimestamp")
})
public class AnomalyTaskSpec extends AbstractBaseEntity {

  @Column(name = "job_id", nullable = false)
  private long jobId;

  @Enumerated(EnumType.STRING)
  @Column(name = "task_type", nullable = false)
  private TaskType taskType;

  @Column(name = "worker_id")
  private Long workerId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private TaskStatus status;

  @Column(name = "task_start_time")
  private long taskStartTime;

  @Column(name = "task_end_time")
  private long taskEndTime;

  @Column(name = "task_info", nullable = false)
  private String taskInfo;

  @Column(name = "last_modified", nullable = false)
  private Timestamp lastModified;

  public long getJobId() {
    return jobId;
  }

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

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  public long getTaskStartTime() {
    return taskStartTime;
  }

  public void setTaskStartTime(long startTime) {
    this.taskStartTime = startTime;
  }

  public long getTaskEndTime() {
    return taskEndTime;
  }

  public void setTaskEndTime(long endTime) {
    this.taskEndTime = endTime;
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


  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyTaskSpec)) {
      return false;
    }
    AnomalyTaskSpec af = (AnomalyTaskSpec) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(jobId, af.getJobId()) && Objects
        .equals(status, af.getStatus()) && Objects.equals(taskStartTime, af.getTaskStartTime())
        && Objects.equals(taskEndTime, af.getTaskEndTime()) && Objects
        .equals(taskInfo, af.getTaskInfo());
  }

  @Override public int hashCode() {
    return Objects.hash(getId(), jobId, status, taskStartTime, taskEndTime, taskInfo);
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("id", getId()).add("jobId", jobId)
        .add("status", status).add("startTime", taskStartTime).add("endTime", taskEndTime)
        .add("taskInfo", taskInfo).add("lastModified", lastModified).toString();
  }
}
