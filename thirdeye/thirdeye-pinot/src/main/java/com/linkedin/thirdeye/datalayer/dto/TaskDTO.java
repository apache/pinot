package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskType;
import java.sql.Timestamp;

/**
 * This class corresponds to anomaly tasks. An execution of an anomaly function creates an anomaly job, which in turn
 * spawns into 1 or more anomaly tasks. The anomaly tasks are picked by the workers
 */
public class TaskDTO extends AbstractDTO {

  private JobDTO job;

  private TaskType taskType;

  private Long workerId;

  private String jobName;

  private TaskStatus status;

  private long taskStartTime;

  private long taskEndTime;

  private String taskInfo;

  private Timestamp lastModified;

  private int version;


  public JobDTO getJob() {
    return job;
  }

  public void setJob(JobDTO job) {
    this.job = job;
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

  public int getVersion() {
    return version;
  }
}
