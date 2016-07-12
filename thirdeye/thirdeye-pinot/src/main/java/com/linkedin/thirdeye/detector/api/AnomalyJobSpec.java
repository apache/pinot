package com.linkedin.thirdeye.detector.api;

import java.util.List;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.JobRunner.JobStatus;

/**
 * This class corresponds to an anomaly job. An anomaly job is created for every execution of an anomaly function spec
 * An anomaly job consists of 1 or more anomaly tasks
 */
@Entity
@Table(name = "anomaly_jobs")
@NamedQueries({
    @NamedQuery(name = "com.linkedin.thirdeye.anomaly.AnomalyJobSpec#findAll", query = "SELECT af FROM AnomalyJobSpec af")
})
public class AnomalyJobSpec {
  @Id
  @Column(name = "job_execution_id")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long jobExecutionId;

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private JobStatus status;

  @Column(name = "schedule_start_time", nullable = false)
  private long scheduleStartTime;

  @Column(name = "schedule_end_time", nullable = false)
  private long scheduleEndTime;

  @Column(name = "window_start_time", nullable = false)
  private long windowStartTime;

  @Column(name = "window_end_time", nullable = false)
  private long windowEndTime;

  @OneToMany(fetch = FetchType.LAZY)
  @JoinColumn(name = "job_execution_id")
  private List<AnomalyTaskSpec> anomalyTasks;

  public AnomalyJobSpec() {
  }


  public long getJobExecutionId() {
    return jobExecutionId;
  }


  public void setJobExecutionId(long jobExecutionId) {
    this.jobExecutionId = jobExecutionId;
  }


  public String getJobName() {
    return jobName;
  }


  public void setJobName(String jobName) {
    this.jobName = jobName;
  }


  public JobStatus getStatus() {
    return status;
  }


  public void setStatus(JobStatus status) {
    this.status = status;
  }


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



  public long getWindowStartTime() {
    return windowStartTime;
  }


  public void setWindowStartTime(long windowStartTime) {
    this.windowStartTime = windowStartTime;
  }


  public long getWindowEndTime() {
    return windowEndTime;
  }


  public void setWindowEndTime(long windowEndTime) {
    this.windowEndTime = windowEndTime;
  }


  public List<AnomalyTaskSpec> getAnomalyTasks() {
    return anomalyTasks;
  }


  public void setAnomalyTasks(List<AnomalyTaskSpec> anomalyTasks) {
    this.anomalyTasks = anomalyTasks;
  }


  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AnomalyJobSpec)) {
      return false;
    }
    AnomalyJobSpec af = (AnomalyJobSpec) o;
    return Objects.equals(jobExecutionId, af.getJobExecutionId()) && Objects.equals(jobName, af.getJobName())
        && Objects.equals(status, af.getStatus()) && Objects.equals(scheduleStartTime, af.getScheduleStartTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobExecutionId, jobName, status, scheduleStartTime);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jobExecutionId", jobExecutionId).add("jobName", jobName)
        .add("status", status).add("startTime", scheduleStartTime).add("endTime", scheduleEndTime).toString();
  }
}
