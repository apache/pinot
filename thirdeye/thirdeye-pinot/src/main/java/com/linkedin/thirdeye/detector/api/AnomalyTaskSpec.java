package com.linkedin.thirdeye.detector.api;

import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;

/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly job, which in turn
 * spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the workers
 */
@Entity
@Table(name = "anomaly_tasks")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findAll", query = "SELECT at FROM AnomalyTaskSpec at"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByJobExecutionId", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.jobExecutionId = :jobExecutionId"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#findByStatusOrderByCreateTimeAscending", query = "SELECT at FROM AnomalyTaskSpec at WHERE at.status = :status order by at.taskStartTime asc"),
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyTaskSpec#updateStatus", query = "UPDATE AnomalyTaskSpec SET status = :newStatus WHERE status = :oldStatus and taskId = :taskId")
})
public class AnomalyTaskSpec {
  @Id
  @Column(name = "task_id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long taskId;

  @Column(name = "job_execution_id", nullable = false)
  private long jobExecutionId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private JobStatus status;

  @Column(name = "task_start_time", nullable = false)
  private long taskStartTime;

  @Column(name = "task_end_time", nullable = false)
  private long taskEndTime;

  @Column(name = "task_info", nullable = false)
  private String taskInfo;

  public AnomalyTaskSpec() {
  }



  public long getTaskId() {
    return taskId;
  }



  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }



  public long getJobExecutionId() {
    return jobExecutionId;
  }



  public void setJobName(String jobName) {
    this.jobName = jobName;
  }


  public String getJobName() {
    return jobName;
  }



  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }



  public JobStatus getStatus() {
    return status;
  }



  public void setStatus(JobStatus status) {
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



  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyTaskSpec)) {
      return false;
    }
    AnomalyTaskSpec af = (AnomalyTaskSpec) o;
    return Objects.equals(taskId, af.getTaskId()) && Objects.equals(jobExecutionId, af.getJobExecutionId())
        && Objects.equals(status, af.getStatus()) && Objects.equals(taskStartTime, af.getTaskStartTime())
        && Objects.equals(taskEndTime, af.getTaskEndTime()) && Objects.equals(taskInfo, af.getTaskInfo());
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, jobExecutionId, status, taskStartTime, taskEndTime, taskInfo);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("taskId", taskId).add("jobExecutionId", jobExecutionId)
        .add("status", status).add("startTime", taskStartTime).add("endTime", taskEndTime).add("taskInfo", taskInfo).toString();
  }
}
