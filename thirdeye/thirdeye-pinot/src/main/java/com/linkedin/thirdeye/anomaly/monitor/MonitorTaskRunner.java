package com.linkedin.thirdeye.anomaly.monitor;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.job.JobConstants.JobStatus;
import com.linkedin.thirdeye.anomaly.monitor.MonitorConstants.MonitorType;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.datalayer.dto.JobDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

public class MonitorTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MonitorJobRunner.class);

  private DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {

    MonitorTaskInfo monitorTaskInfo = (MonitorTaskInfo) taskInfo;
    MonitorType monitorType = monitorTaskInfo.getMonitorType();
    if (monitorType.equals(MonitorType.UPDATE)) {
      executeMonitorUpdate(monitorTaskInfo);
    } else if (monitorType.equals(MonitorType.EXPIRE)) {
      executeMonitorExpire(monitorTaskInfo);
    } else {
      throw new UnsupportedOperationException("Monitor task must be of type UPDATE/EXPIRE, found " + monitorType);
    }
    return null;
  }

  private void executeMonitorUpdate(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor update {}", monitorTaskInfo);
    try {

      // All jobs with status SCHEDULED
      Set<Long> scheduledJobIds = findAllJobsWithStatusScheduled();

      // All incomplete jobs with status SCHEDULED
      Set<Long> incompleteScheduledJobIds = findIncompleteJobsWithStatusScheduled();

      // All finished jobs with status SCHEDULED
      scheduledJobIds.removeAll(incompleteScheduledJobIds);

      if (!scheduledJobIds.isEmpty()) {
        DAO_REGISTRY.getJobDAO().updateStatusAndJobEndTimeForJobIds(scheduledJobIds, JobStatus.COMPLETED, System.currentTimeMillis());
        LOG.info("COMPLETED jobs {}", scheduledJobIds);
      }

    } catch (Exception e) {
      LOG.error("Exception in monitor update task", e);
    }
  }

  private void executeMonitorExpire(MonitorTaskInfo monitorTaskInfo) {
    LOG.info("Execute monitor expire {}", monitorTaskInfo);

    // Delete completed jobs and tasks that are expired (14 days by default)
    try {
      // CAUTION: Fist delete tasks then jobs, as task has a foreign key.
      int completedJobRetentionDays = monitorTaskInfo.getCompletedJobRetentionDays();
      int deletedCompletedTasks = DAO_REGISTRY.getTaskDAO()
          .deleteRecordsOlderThanDaysWithStatus(completedJobRetentionDays, TaskStatus.COMPLETED);
      int deletedCompletedJobs =
          DAO_REGISTRY.getJobDAO().deleteRecordsOlderThanDaysWithStatus(completedJobRetentionDays, JobStatus.COMPLETED);
      LOG.info("Deleted {} completed jobs and {} completed tasks that are older than {} days.", deletedCompletedJobs,
          deletedCompletedTasks, completedJobRetentionDays);
    } catch (Exception e) {
      LOG.error("Exception when expiring jobs and tasks.", e);
    }

    // Delete all types of jobs and tasks that are expired (30 days by default)
    try {
      // CAUTION: Fist delete tasks then jobs, as task has a foreign key.
      int jobRetentionDays = monitorTaskInfo.getDefaultRetentionDays();
      int deletedTasks = DAO_REGISTRY.getTaskDAO().deleteRecordsOlderThanDays(jobRetentionDays);
      int deletedJobs = DAO_REGISTRY.getJobDAO().deleteRecordsOlderThanDays(jobRetentionDays);
      LOG.info("Deleted {} jobs and {} tasks that are older than {} days.", deletedTasks, deletedJobs,
          jobRetentionDays);
    } catch (Exception e) {
      LOG.error("Exception when expiring jobs and tasks.", e);
    }

    // Delete expired detection status (7 days by default)
    try {
      int deletedDetectionStatus = DAO_REGISTRY.getDetectionStatusDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getDetectionStatusRetentionDays());
      LOG.info("Deleted {} detection status that are older than {} days.", deletedDetectionStatus,
          monitorTaskInfo.getDetectionStatusRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring detection status.", e);
    }

    // Delete expired data completeness entries (30 days)
    try {
      int deletedDetectionStatus = DAO_REGISTRY.getDataCompletenessConfigDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getDefaultRetentionDays());
      LOG.info("Deleted {} data completeness entries that are older than {} days.", deletedDetectionStatus,
          monitorTaskInfo.getDefaultRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring data completeness entries.", e);
    }

    // Delete expired raw anomalies (30 days by default)
    try {
      int deletedRawAnomalies = DAO_REGISTRY.getRawAnomalyResultDAO()
          .deleteRecordsOlderThanDays(monitorTaskInfo.getRawAnomalyRetentionDays());
      LOG.info("Deleted {} raw anomalies that are older than {} days.", deletedRawAnomalies,
          monitorTaskInfo.getRawAnomalyRetentionDays());
    } catch (Exception e) {
      LOG.error("Exception when expiring raw anomalies.", e);
    }
  }


  private Set<Long> findAllJobsWithStatusScheduled() {
    Set<Long> scheduledJobIds = new HashSet<>();
    try {
      List<JobDTO> scheduledJobs = DAO_REGISTRY.getJobDAO().findByStatus(JobStatus.SCHEDULED);
      for (JobDTO job : scheduledJobs) {
        scheduledJobIds.add(job.getId());
      }
    } catch (Exception e) {
      LOG.error("Exception in finding jobs with status scheduled", e);
    }
    return scheduledJobIds;
  }

  private Set<Long> findIncompleteJobsWithStatusScheduled() {
    Set<Long> incompleteScheduledJobIds = new HashSet<>();
    List<TaskDTO> incompleteTasks = DAO_REGISTRY.getTaskDAO().findByStatusNotIn(TaskStatus.COMPLETED);
    for (TaskDTO task : incompleteTasks) {
      incompleteScheduledJobIds.add(task.getJobId());
    }
    return incompleteScheduledJobIds;
  }
}
