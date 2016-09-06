package com.linkedin.thirdeye.datalayer.pojo;

import java.sql.Timestamp;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;

import com.google.common.base.MoreObjects;
import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;

/**
 * This class corresponds to an anomaly job. An anomaly job is created for every execution of an
 * anomaly function spec An anomaly job consists of 1 or more anomaly tasks
 */
@Entity
@Table(name = "anomaly_jobs")

public class JobBean extends AbstractBean {

  @Column(name = "job_name", nullable = false)
  private String jobName;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false)
  private JobStatus status;

  @Column(name = "schedule_start_time")
  private long scheduleStartTime;

  @Column(name = "schedule_end_time")
  private long scheduleEndTime;

  @Column(name = "window_start_time")
  private long windowStartTime;

  @Column(name = "window_end_time")
  private long windowEndTime;

  @Column(name = "last_modified", insertable = false, updatable = false,
      columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
  private Timestamp lastModified;

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

  public Timestamp getLastModified() {
    return lastModified;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof JobBean)) {
      return false;
    }
    JobBean af = (JobBean) o;
    return Objects.equals(getId(), af.getId()) && Objects.equals(jobName, af.getJobName())
        && Objects.equals(status, af.getStatus())
        && Objects.equals(scheduleStartTime, af.getScheduleStartTime());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), jobName, status, scheduleStartTime);
  }

  @Override
  public String toString() {

    return MoreObjects.toStringHelper(this).add("id", getId()).add("jobName", jobName)
        .add("status", status).add("scheduleStartTime", scheduleStartTime)
        .add("scheduleEndTime", scheduleEndTime).add("windowStartTime", windowStartTime)
        .add("windowEndTime", windowEndTime).add("lastModified", lastModified).toString();
  }
}
